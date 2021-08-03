use dev_portal;
select VMName,
       min(case when action = 'in'  then dt end) as login_time,
       max(case when action = 'out' then dt end) as logout_time
from (select t.*,
             (@grp := if(@VMName = VMName, @grp,
                         if(@VMName := VMName, @grp + 1, @grp + 1)
                        )
             ) as grp
      from (select VMName, Login as dt, 'in' as action
            from Logins
            union all
            select VMName, Logout, 'out'
            from Logouts
            order by 1, 2
           ) t cross join
           (select @VMName := -1, @grp := -1) params
     ) t
group by VMName, grp
order by VMName, grp;

-- use dev_portal;
-- call update_vmuptime_table('Alteon Ansible Automation-Persistent-(chensa)-2021-07-14-14:07:16:299183', 1);

-- use dev_portal;
-- drop trigger trigger_update_vmuptime_table_in_creation;


-- DELIMITER $$
-- CREATE TRIGGER trigger_update_vmuptime_table_in_creation
--     AFTER INSERT
--     ON Reservations FOR EACH ROW
-- BEGIN
-- 	IF NEW.VMName LIKE CONCAT('%-Persistent-%') THEN
-- 		call update_vmuptime_table(NEW.VMName, 3);
-- 	END IF;
-- END$$
-- DELIMITER ;


-- create table VMUptime_Logs(
-- ID INT NOT NULL AUTO_INCREMENT,
-- VMName varchar(256) NOT NULL,
-- Log DATETIME,
-- Operation varchar(40),
-- Primary key (ID),
-- FOREIGN KEY (VMName) REFERENCES Reservations(VMName))
-- Engine InnoDB;

-- DELIMITER $$
-- CREATE PROCEDURE `update_vmuptime_table`(IN vm varchar (256),
-- IN op int
-- )
-- BEGIN
-- 	IF op = 0 THEN
-- 		INSERT INTO VMUptime_Logs VALUES (NULL,vm, NOW(), 'Powered-On');		
-- 	ELSEIF op = 1 THEN
-- 		INSERT INTO VMUptime_Logs VALUES (NULL,vm, NOW(), 'Suspended');
-- 	ELSEIF op = 2 THEN
-- 		INSERT INTO VMUptime_Logs VALUES (NULL,vm, NOW(), 'Deleted');
-- 	END IF;
-- END$$

-- DELIMITER ;



##########################################################
#### final query for statistic for persistent (Final) ####
##########################################################

SELECT substring_index(substring_index(mod_reservations.VMName, '-(', -1),')-', 1) AS 'Owner',
mod_reservations.Lab,
substring_index(mod_reservations.VMName, '-', -1-3) AS 'Lab Creation (UTC)',
mod_reservations.Active AS 'Status',
  CONCAT(
    FLOOR(differences.seconds / 3600 / 24), ' days ',
    FLOOR(differences.hours_part / 3600), ' hours ',
    FLOOR(differences.minutes_part / 60), ' minutes ',
    differences.seconds_part, 'seconds'
  ) AS `TotaL Duration`,
   CONCAT(ROUND(((UPTime_Logs.sum_time_deff/differences.seconds)*100), 1),'%') AS `Powered-on (%)`
, CONCAT(ROUND(((Logins_logs.sum_time_deff/UPTime_Logs.sum_time_deff)*100), 1),'%') AS `Login/Powered-on (%)`
FROM (select Distinct VMName, MAX(sum_time_deff) AS sum_time_deff from (SELECT DISTINCT t.VMName,SUM(t.Time_Deff) AS 'sum_time_deff'
		FROM (SELECT t.VMName AS 'VMName',
       TIMESTAMPDIFF(SECOND, MAX(CASE WHEN action = 'in'  THEN dt end), MIN(CASE WHEN action = 'out' THEN dt end)) AS 'Time_Deff'
			FROM (SELECT t.*,
             (@grp := IF(@VMName = VMName, IF(action = 'in', @grp + 1, @grp ),
                         IF(@VMName := VMName, @grp + 1, @grp + 1)
                        )
             ) AS grp
      FROM (SELECT VMName, Login AS dt, 'in' AS action
            FROM Logins
            WHERE VMName LIKE CONCAT('%-Persistent-%')
            UNION ALL
            SELECT VMName, Logout, 'out'
            FROM Logouts
            WHERE VMName LIKE CONCAT('%-Persistent-%')
            UNION ALL
            SELECT DISTINCT Logins.VMName, UTC_TIMESTAMP () AS dt, 'out' AS 'action'
            FROM Logins, Reservations
            WHERE Logins.VMName = Reservations.VMName AND Reservations.End >= UTC_TIMESTAMP() AND Logins.VMName LIKE CONCAT('%-Persistent-%')
            ORDER BY 1, 2
           ) t CROSS JOIN
           (SELECT @VMName := -1, @grp := -1) params
     ) t
GROUP BY VMName, grp
ORDER BY VMName, grp
) AS t
GROUP BY t.VMName
UNION
SELECT DISTINCT VMName, 0 AS 'sum_time_deff' FROM VMUptime_Logs
GROUP BY VMName) AS t
GROUP BY t.VMName
) AS Logins_logs ,
(SELECT t.VMName, SUM(t.Time_Deff) AS 'sum_time_deff'
		FROM (SELECT t.VMName,
       TIMESTAMPDIFF(SECOND, MAX(CASE WHEN action = 'in' THEN dt end), MIN(CASE WHEN action = 'out' THEN dt end)) AS 'Time_Deff'
FROM (SELECT t.*,
             (@grp := IF(@VMName = VMName, IF(action = 'in', @grp + 1, @grp ),
                         IF(@VMName := VMName, @grp + 1, @grp + 1)  
                        )
             ) as grp
      FROM (SELECT VMName, Log AS dt, IF(Operation IN ('Powered-On', 'Created'),'in', 'out') AS 'action'
			FROM VMUptime_Logs
			UNION ALL
            SELECT DISTINCT VMUptime_Logs.VMName, UTC_TIMESTAMP () AS dt, 'out' AS 'action'
            FROM VMUptime_Logs, Reservations
            WHERE VMUptime_Logs.VMName = Reservations.VMName AND Reservations.End >= UTC_TIMESTAMP()
            ORDER BY 1, 2
           ) t CROSS JOIN
           (SELECT @VMName := -1, @grp := -1) params
     ) AS t
GROUP BY VMName, grp
ORDER BY VMName, grp) AS t
GROUP BY VMName) AS UPTime_Logs,
(SELECT VMName,substring_index(VMName, '-', -1-3) AS Start,IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS active, Lab, Email FROM Reservations WHERE Reservations.VMName LIKE CONCAT('%-Persistent-%')) AS mod_reservations,
(SELECT
    difference_in_seconds.VMName,
    difference_in_seconds.Start,
    difference_in_seconds.End,
    difference_in_seconds.seconds,
    MOD(difference_in_seconds.seconds, 60) AS seconds_part,
    MOD(difference_in_seconds.seconds, 3600) AS minutes_part,
    MOD(difference_in_seconds.seconds, 3600 * 24) AS hours_part
  FROM (  SELECT
    mod_reservations.VMName AS VMName,
    mod_reservations.Start AS Start,
    IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End) AS End,
    TIMESTAMPDIFF(SECOND, mod_reservations.Start, IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End)) AS seconds
  FROM (SELECT Reservations.VMName, cast(substring_index(Reservations.VMName, '-', -1-3) as datetime) AS Start,IF(Log IS NULL, END, Log) AS End FROM Reservations LEFT JOIN (SELECT VMName, Log From  VMUptime_Logs WHERE Operation = 'Deleted') AS Deleted_Times ON Deleted_Times.VMName =  Reservations.VMName WHERE Reservations.VMName LIKE CONCAT('%-Persistent-%')) AS mod_reservations) AS difference_in_seconds) AS differences
WHERE
mod_reservations.VMName = Logins_logs.VMName AND
UPTime_Logs.VMName = Logins_logs.VMName AND
mod_reservations.VMName = UPTime_Logs.VMName AND
differences.VMName = mod_reservations.VMName AND
Lab IN ('Defense Flow','SSL Inspection','Alteon and Analytics','Alteon Ansible Automation','Alteon GEL Automation','Appwall','KWAF','KWAF - External Authorization Mode','KWAF - Internal Authorization Mode','Virtual DefensePro','Alteon Cloud Controller') AND
SUBSTRING_INDEX(mod_reservations.Email, '@', 1) In ('anandm','andylau','arunp','danielo','deanm','esteban.pierotti','gregd','haraldb','idanso','jesus.rojas','juann','krishna.gullapalli','maory','massimilianom','noaho','prateek.vishwanath','rajeev.shrestha','ricardom','shaheedb','siddharth.iyer','taly','virg.santos','vladimirv','xiaw','yarivk') AND
mod_reservations.active IN ('Active', 'Deleted')
GROUP BY Logins_logs.VMName, mod_reservations.VMName, differences.VMName, UPTime_Logs.VMName, `TotaL Duration`, `Powered-on (%)`, `Login/Powered-on (%)`
ORDER BY Status;

########################################################
#### final query for statistic for Regular (Final) #####
#########################################################

SELECT substring_index(substring_index(Logins_logs.VMName, '-(', -1),')-', 1) AS 'Owner',
mod_reservations.Lab,
substring_index(Logins_logs.VMName, '-', -1-3) AS 'Lab Creation (UTC)',
mod_reservations.Active AS 'Status',
  CONCAT(
    FLOOR(differences.seconds / 3600 / 24), ' days ',
    FLOOR(differences.hours_part / 3600), ' hours ',
    FLOOR(differences.minutes_part / 60), ' minutes ',
    differences.seconds_part, ' seconds'
  ) AS 'TotaL Duration',
   CONCAT(ROUND(((Logins_logs.sum_time_deff/differences.seconds)*100), 1),'%') AS 'Active (%)'
FROM (SELECT t.VMName,SUM(t.Time_Deff) AS 'sum_time_deff'
		FROM (SELECT t.VMName,
       TIMESTAMPDIFF(SECOND, MAX(CASE WHEN action = 'in'  THEN dt end), MIN(CASE WHEN action = 'out' THEN dt end)) AS 'Time_Deff'
FROM (SELECT t.*,
             (@grp := IF(@VMName = VMName, IF(action = 'in', @grp + 1, @grp ),
                         IF(@VMName := VMName, @grp + 1, @grp + 1)
                        )
             ) AS grp
      FROM (SELECT VMName, Login AS dt, 'in' AS action
            FROM Logins
            UNION ALL
            SELECT VMName, Logout, 'out'
            FROM Logouts
            WHERE VMName NOT LIKE CONCAT('%-Persistent-%')
            UNION ALL
            SELECT DISTINCT Logins.VMName, UTC_TIMESTAMP () AS dt, 'out' AS 'action'
            FROM Logins, Reservations
            WHERE Logins.VMName = Reservations.VMName AND Reservations.End >= UTC_TIMESTAMP() AND Logins.VMName NOT LIKE CONCAT('%-Persistent-%')
            ORDER BY 1, 2
           ) t CROSS JOIN
           (SELECT @VMName := -1, @grp := -1) params
     ) t
GROUP BY VMName, grp
ORDER BY VMName, grp) AS t
GROUP BY VMName) AS Logins_logs,
(SELECT VMName,Start,End, IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS active, Lab, Email FROM Reservations) AS mod_reservations,
(SELECT
    difference_in_seconds.VMName,
    difference_in_seconds.Start,
    difference_in_seconds.End,
    difference_in_seconds.seconds,
    MOD(difference_in_seconds.seconds, 60) AS seconds_part,
    MOD(difference_in_seconds.seconds, 3600) AS minutes_part,
    MOD(difference_in_seconds.seconds, 3600 * 24) AS hours_part
  FROM (  SELECT
    mod_reservations.VMName AS VMName,
    mod_reservations.Start AS Start,
    IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End) AS End,
    TIMESTAMPDIFF(SECOND, mod_reservations.Start, IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End)) AS seconds
  FROM (SELECT VMName,Start,End FROM Reservations) AS mod_reservations) AS difference_in_seconds) AS differences
WHERE mod_reservations.VMName = Logins_logs.VMName AND
differences.VMName = mod_reservations.VMName AND
Logins_logs.VMName NOT LIKE CONCAT('%-Persistent-%') AND
Lab IN ('Appwall') AND
SUBSTRING_INDEX(mod_reservations.Email, '@', 1) In ('idanso') AND
mod_reservations.active IN ('Active', 'Inactive')
GROUP BY Logins_logs.VMName
ORDER BY Status;
 
 
#########################################
#### final query for statistic (Old) ####
#########################################
#use portal;
SELECT UPTime_Logs.VMName,
mod_reservations.active,
CONCAT(ROUND(((Logins_logs.sum_time_deff/UPTime_Logs.sum_time_deff)*100), 1),'%') AS 'Percent'
FROM (SELECT t.VMName,SUM(t.Time_Deff) AS 'sum_time_deff'
		FROM (SELECT t.VMName,
       TIMESTAMPDIFF(MINUTE, MAX(CASE WHEN action = 'in'  THEN dt end), MIN(CASE WHEN action = 'out' THEN dt end)) AS 'Time_Deff'
FROM (SELECT t.*,
             (@grp := IF(@VMName = VMName, IF(action = 'in', @grp + 1, @grp ),
                         IF(@VMName := VMName, @grp + 1, @grp + 1)
                        )
             ) AS grp
      FROM (SELECT VMName, Login AS dt, 'in' AS action
            FROM Logins
            UNION ALL
            SELECT VMName, Logout, 'out'
            FROM Logouts
            WHERE VMName LIKE CONCAT('%-Persistent-%')
            UNION ALL
            SELECT DISTINCT Logins.VMName, UTC_TIMESTAMP () AS dt, 'out' AS 'action'
            FROM Logins, Reservations
            WHERE Logins.VMName = Reservations.VMName AND Reservations.End >= UTC_TIMESTAMP() AND Logins.VMName LIKE CONCAT('%-Persistent-%')
            ORDER BY 1, 2
           ) t CROSS JOIN
           (SELECT @VMName := -1, @grp := -1) params
     ) t
GROUP BY VMName, grp
ORDER BY VMName, grp) AS t
GROUP BY VMName) AS Logins_logs,
(SELECT t.VMName, SUM(t.Time_Deff) AS 'sum_time_deff'
		FROM (SELECT t.VMName,
       TIMESTAMPDIFF(MINUTE, MAX(CASE WHEN action = 'in' THEN dt end), MIN(CASE WHEN action = 'out' THEN dt end)) AS 'Time_Deff'
FROM (SELECT t.*,
             (@grp := IF(@VMName = VMName, IF(action = 'in', @grp + 1, @grp ),
                         IF(@VMName := VMName, @grp + 1, @grp + 1)  
                        )
             ) as grp
      FROM (SELECT VMName, Log AS dt, IF(Operation IN ('Powered-On', 'Created'),'in', 'out') AS 'action'
			FROM VMUptime_Logs
			UNION ALL
            SELECT DISTINCT VMUptime_Logs.VMName, UTC_TIMESTAMP () AS dt, 'out' AS 'action'
            FROM VMUptime_Logs, Reservations
            WHERE VMUptime_Logs.VMName = Reservations.VMName AND Reservations.End >= UTC_TIMESTAMP()
            ORDER BY 1, 2
           ) t CROSS JOIN
           (SELECT @VMName := -1, @grp := -1) params
     ) AS t
GROUP BY VMName, grp
ORDER BY VMName, grp) AS t
GROUP BY VMName) AS UPTime_Logs,
(SELECT VMName, IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Inactive') AS active FROM Reservations) AS mod_reservations
WHERE UPTime_Logs.VMName = Logins_logs.VMName AND
mod_reservations.VMName = Logins_logs.VMName AND
mod_reservations.VMName = UPTime_Logs.VMName AND
mod_reservations.active IN ('Active')
GROUP BY UPTime_Logs.VMName;
############ Test ################
 
SELECT
  differences.id,
  differences.departure,
  differences.arrival,
  CONCAT(
    FLOOR(differences.seconds / 3600 / 24), ' days ',
    FLOOR(differences.hours_part / 3600), ' hours ',
    FLOOR(differences.minutes_part / 60), ' minutes ',
    differences.seconds_part, ' seconds'
  ) AS difference
FROM   (SELECT
    difference_in_seconds.id,
    difference_in_seconds.departure,
    difference_in_seconds.arrival,
    difference_in_seconds.seconds,
    MOD(difference_in_seconds.seconds, 60) AS seconds_part,
    MOD(difference_in_seconds.seconds, 3600) AS minutes_part,
    MOD(difference_in_seconds.seconds, 3600 * 24) AS hours_part
  FROM (  SELECT
    mod_reservations.VMName AS id,
    mod_reservations.Start AS departure,
    IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End) AS arrival,
    TIMESTAMPDIFF(SECOND, mod_reservations.Start, IF(UTC_TIME() < mod_reservations.End, UTC_TIME(), mod_reservations.End)) AS seconds
  FROM mod_reservations) AS difference_in_seconds) AS differences;
###################################
########### RAW Logs ##############
###################################
SELECT
    substring_index(substring_index(unioned.VMName, '-(', -1),')-', 1) AS 'Owner',
    substring_index(unioned.VMName, '-', 1) AS 'Lab',
    mod_reservations.active,
    mod_reservations.Type,
    substring_index(unioned.VMName, '-', -1-3) AS 'Lab Creation (UTC)',
    DATE_FORMAT(unioned.Log, '%Y/%m/%d %H:%i:%S') as 'Timestamp (UTC)', Operation, op_code
FROM(
  SELECT VMName, Login AS Log, 'Login' AS 'Operation', 0 AS op_code FROM Logins
	  UNION ALL
  SELECT VMName, Logout AS Log, 'Logout' AS 'Operation', 1 AS op_code FROM Logouts
    UNION ALL
  SELECT VMName,  Log AS Log, Operation, IF(Operation = 'Created', 2, IF(Operation = 'powered-on', 3, IF(Operation = 'suspended', 4, 5))) AS 'op_code' FROM VMUptime_Logs) AS unioned
  ,(select VMName, LAB, Email,  IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS active, IF(Reservations.VMName LIKE CONCAT('%-Persistent-%'), 'Customized', 'On-Demand') AS 'Type' FROM Reservations) AS mod_reservations
WHERE
  unioned.VMName = mod_reservations.VMName AND
  Lab IN ('Appwall') AND
  SUBSTRING_INDEX(Email, '@', 1) In ('idanso')
  AND mod_reservations.active in ('Inactive')
  AND mod_reservations.type in ('Customized')
ORDER BY Log DESC;

############################################
############## not working #################
############################################

select * from (
select t.VMName,
       DATE_FORMAT(min(case when action = 'in'  then dt end), '%Y/%m/%d %H:%i:%S') as 'login time',
       DATE_FORMAT(min(case when action = 'out' then dt end), '%Y/%m/%d %H:%i:%S') as 'logout time'
from (select t.*,
             (@grp := if(@VMName = VMName, if(action = 'in', @grp + 1, @grp ),
                         if(@VMName := VMName, @grp + 1, @grp + 1)
                        )
             ) as grp
      from (select VMName, Login as dt, 'in' as action
            from Logins
            union all
            select VMName, Logout, 'out'
            from Logouts
            order by 1, 2
           ) t cross join
           (select @VMName := -1, @grp := -1) params
     ) t, Reservations
WHERE t.VMName = Reservations.VMName AND Lab IN ('Appwall') and SUBSTRING_INDEX(Email, '@', 1) In ('idanso')
group by VMName, grp
UNION ALL
select t.VMName,
       DATE_FORMAT(min(case when action = 'in'  then dt end), '%Y/%m/%d %H:%i:%S') as 'login time',
       DATE_FORMAT(min(case when action = 'out' then dt end), '%Y/%m/%d %H:%i:%S') as 'logout time'
from (select t.*,
             (@grp := if(@VMName = VMName, if(action = 'in', @grp + 1, @grp ),
                         if(@VMName := VMName, @grp + 1, @grp + 1)  
                        )
             ) as grp
      from (select VMName, Log as dt, if(Operation in ('Powered-On', 'Created'),'in', 'out') AS 'action'
			FROM VMUptime_Logs
            order by 1, 2
           ) t cross join
           (select @VMName := -1, @grp := -1) params
     ) t, Reservations
WHERE t.VMName = Reservations.VMName AND Lab IN ('Appwall') and SUBSTRING_INDEX(Email, '@', 1) In ('idanso')
group by VMName, grp
order by VMName, grp);

##################

SELECT 
    @diff:=ABS( UNIX_TIMESTAMP("2021-01-01 21:24:25") - UNIX_TIMESTAMP() ) , 
    CONCAT(CAST(@days := IF(@diff/86400 >= 1, floor(@diff / 86400 ),0) AS SIGNED),":",  
    CAST(@hours := IF(@diff/3600 >= 1, floor((@diff:=@diff-@days*86400) / 3600),0) AS SIGNED), ":", 
    CAST(@minutes := IF(@diff/60 >= 1, floor((@diff:=@diff-@hours*3600) / 60),0) AS SIGNED), ":", 
    CAST(@diff-@minutes*60 AS SIGNED)) AS 'time';

SELECT VMName,substring_index(VMName, '-', -1-3) AS Start,
IF(Reservations.End >= UTC_TIMESTAMP(),End, (SELECT Log From  (SELECT VMName, Log FROM VMUptime_Logs WHERE Operation = 'Deleted' AND VMName = ) AS t WHERE VMName = t.VMName)) AS End,
 IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS active,
 Lab,
 Email
 FROM Reservations
 WHERE Reservations.VMName LIKE CONCAT('%-Persistent-%')