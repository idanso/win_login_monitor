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
ORDER BY Status, mod_reservations.Start DESC;

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
ORDER BY Status, mod_reservations.Start DESC;
 
 
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
######### labs report bu users #############
############################################

SELECT Employees.Full_Name AS 'Full Name',
Reservations.Lab , substring_index(Reservations.VMName, '-', -1-3) AS 'Lab Creation (UTC)',
IF(Reservations.End >= UTC_TIMESTAMP(),'', Reservations.End) AS 'Lab Deletion (UTC)',
Reservations.State,
Reservations.Type,
Employees.Region AS 'Region',
Employees.Location AS 'Location',
Employees.Manager AS 'Manager',
Employees.Business_Unit AS 'Group'
FROM
(SELECT Lab, VMName, End, Email, IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS 'State', IF(Reservations.VMName LIKE CONCAT('%-Persistent-%'), 'Customized', 'On-Demand') AS 'Type' FROM Reservations) AS Reservations
LEFT JOIN
Employees ON Employees.Email = Reservations.Email
WHERE
Region IN ('EMEA&CALA', 'APAC', 'North America') AND
Lab IN ('Alteon and Analytics', 'Alteon GEL Automation', 'Appwall', 'Defense Flow') AND
Manager IN ('Kontsevoy, Igor', 'Bacchus, Shaheed', 'Katz, Yariv') AND
State IN ('Deleted', 'Active') AND
Type IN ('Customized', 'On-Demand') AND
if('2' = '0', Employees.ASE = False, IF('2' = '1', Employees.ASE = True, Employees.ASE in (select ASE FROM Employees)));

#############################################
######### labs report by Regions #############
#############################################

SELECT
Reservations.Start,
IF(Reservations.End >= UTC_TIMESTAMP(),'', Reservations.End) AS 'Lab Deletion (UTC)',
Reservations.State,
Reservations.Type,

FROM
(SELECT Lab, VMName, End, Email, IF(Reservations.End >= UTC_TIMESTAMP(),'Active', 'Deleted') AS 'State', IF(Reservations.VMName LIKE CONCAT('%-Persistent-%'), 'Customized', 'On-Demand') AS 'Type' FROM Reservations) AS Reservations
LEFT JOIN
Employees ON Employees.Email = Reservations.Email
WHERE
Region IN ('EMEA&CALA', 'APAC', 'North America') AND
Lab IN ('Alteon and Analytics', 'Alteon GEL Automation', 'Appwall', 'Defense Flow') AND
Manager IN ('Kontsevoy, Igor', 'Bacchus, Shaheed', 'Katz, Yariv') AND
State IN ('Deleted', 'Active') AND
Type IN ('Customized', 'On-Demand') AND
if('2' = '0', Employees.ASE = False, IF('2' = '1', Employees.ASE = True, Employees.ASE in (select ASE FROM Employees)));

##############################################################
######## Reservations Summary By user and their labs #########
##############################################################

select Full_Name, Manager, Region, Location, Department, Lab, Reservations.Email, COUNT(VMName) AS 'Reservations Count', DATE_FORMAT(MAX(Start), '%Y/%m/%d %H:%i:%S') AS 'Last Reservations', IF(ASE = 1, 'YES', 'NO') AS 'ASE'
FROM Reservations LEFT JOIN Employees ON Reservations.Email = Employees.Email
WHERE Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594)
Group by Full_Name,Lab, Manager, Region, Location, Department, Email, ASE;

#################################################################
######## Reservations Summary By user and their labs V2 #########
#################################################################

SELECT
Employees.Full_Name, Employees.Manager, Employees.Region, Employees.Location, Employees.Department, IF(Employees.ASE = 1, 'YES', 'NO') AS 'ASE', COUNT(Reservations.VMName) AS 'Reservations Total Count',
IF(AAAnalytics_col.Count IS NULL, 0, AAAnalytics_col.Count) AS 'Alteon and Analytics', IF(AAAnalytics_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAnalytics_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(AAAutomation_col.Count IS NULL, 0, AAAutomation_col.Count) AS 'Alteon Ansible Automation', IF(AAAutomation_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAutomation_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(ACController_col.Count IS NULL, 0, ACController_col.Count) AS 'Alteon Cloud Controller', IF(ACController_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(ACController_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(VDP_col.Count IS NULL, 0, VDP_col.Count) AS 'Virtual DefensePro', IF(VDP_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(VDP_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(SSLI_col.Count IS NULL, 0, SSLI_col.Count) AS 'SSL Inspection', IF(SSLI_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(SSLI_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(APPW_col.Count IS NULL, 0, APPW_col.Count) AS 'Appwall', IF(APPW_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(APPW_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(DF_col.Count IS NULL, 0, DF_col.Count) AS 'Defense Flow', IF(DF_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(DF_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(KWAFEA_col.Count IS NULL, 0, KWAFEA_col.Count) AS 'KWAF - ExtAuth', IF(KWAFEA_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFEA_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(KWAFIM_col.Count IS NULL, 0, KWAFIM_col.Count) AS 'KWAF - Inline Mode', IF(KWAFIM_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFIM_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(GEL_col.Count IS NULL, 0, GEL_col.Count) AS 'Global Elastic License (GEL)', IF(GEL_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(GEL_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'
FROM
Reservations
LEFT JOIN Employees ON Reservations.Email = Employees.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon and Analytics' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS AAAnalytics_col ON Reservations.Email = AAAnalytics_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon Ansible Automation' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS AAAutomation_col ON Reservations.Email = AAAutomation_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS ACController_col ON Reservations.Email = ACController_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Virtual DefensePro' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS VDP_col ON Reservations.Email = VDP_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'SSL Inspection' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS SSLI_col ON Reservations.Email = SSLI_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Appwall' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS APPW_col ON Reservations.Email = APPW_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Defense Flow' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS DF_col ON Reservations.Email = DF_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS KWAFEA_col ON Reservations.Email = KWAFEA_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'KWAF - Inline Mode' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS KWAFIM_col ON Reservations.Email = KWAFIM_col.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS GEL_col ON Reservations.Email = GEL_col.Email
WHERE
Reservations.Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594)
Group by Full_Name, Manager, Region, Location, Department, Reservations.Email, ASE;

#################################################################
######## Reservations Summary By user and their labs V3 #########
#################################################################
# Changes: with last reservations regardless the time range and removed ASE and Department columns

SELECT
Employees.Full_Name, Employees.Manager, Employees.Region, Employees.Location, IF(total_by_time.Count IS NULL, 0, total_by_time.Count) AS 'Reservations Total Count',
IF(AAAnalytics_col.Count IS NULL, 0, AAAnalytics_col.Count) AS 'Alteon and Analytics', IF(AAAnalytics_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAnalytics_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(AAAutomation_col.Count IS NULL, 0, AAAutomation_col.Count) AS 'Alteon Ansible Automation', IF(AAAutomation_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAutomation_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(ACController_col.Count IS NULL, 0, ACController_col.Count) AS 'Alteon Cloud Controller', IF(ACController_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(ACController_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(VDP_col.Count IS NULL, 0, VDP_col.Count) AS 'Virtual DefensePro', IF(VDP_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(VDP_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(SSLI_col.Count IS NULL, 0, SSLI_col.Count) AS 'SSL Inspection', IF(SSLI_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(SSLI_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(APPW_col.Count IS NULL, 0, APPW_col.Count) AS 'Appwall', IF(APPW_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(APPW_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(DF_col.Count IS NULL, 0, DF_col.Count) AS 'Defense Flow', IF(DF_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(DF_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(KWAFEA_col.Count IS NULL, 0, KWAFEA_col.Count) AS 'KWAF - ExtAuth', IF(KWAFEA_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFEA_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(KWAFIM_col.Count IS NULL, 0, KWAFIM_col.Count) AS 'KWAF - Inline Mode', IF(KWAFIM_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFIM_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation',
IF(GEL_col.Count IS NULL, 0, GEL_col.Count) AS 'Global Elastic License (GEL)', IF(GEL_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(GEL_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'
FROM
Employees
LEFT JOIN
Reservations ON Reservations.Email = Employees.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS total_by_time ON Reservations.Email = total_by_time.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Alteon and Analytics' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS AAAnalytics_col ON Reservations.Email = AAAnalytics_col.Email
LEFT JOIN 
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon and Analytics' GROUP BY Email) AS AAAnalytics_col2 ON Reservations.Email = AAAnalytics_col2.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Alteon Ansible Automation' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS AAAutomation_col ON Reservations.Email = AAAutomation_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon Ansible Automation' GROUP BY Email) AS AAAutomation_col2 ON Reservations.Email = AAAutomation_col2.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS ACController_col ON Reservations.Email = ACController_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') GROUP BY Email) AS ACController_col2 ON Reservations.Email = ACController_col2.Email
LEFT JOIN
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Virtual DefensePro' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS VDP_col ON Reservations.Email = VDP_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Virtual DefensePro' GROUP BY Email) AS VDP_col2 ON Reservations.Email = VDP_col2.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'SSL Inspection' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS SSLI_col ON Reservations.Email = SSLI_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'SSL Inspection' GROUP BY Email) AS SSLI_col2 ON Reservations.Email = SSLI_col2.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Appwall' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS APPW_col ON Reservations.Email = APPW_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Appwall' GROUP BY Email) AS APPW_col2 ON Reservations.Email = APPW_col2.Email
LEFT JOIN 
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Defense Flow' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS DF_col ON Reservations.Email = DF_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Defense Flow' GROUP BY Email) AS DF_col2 ON Reservations.Email = DF_col2.Email
LEFT JOIN
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS KWAFEA_col ON Reservations.Email = KWAFEA_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') GROUP BY Email) AS KWAFEA_col2 ON Reservations.Email = KWAFEA_col2.Email
LEFT JOIN
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'KWAF - Inline Mode' AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS KWAFIM_col ON Reservations.Email = KWAFIM_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'KWAF - Inline Mode' GROUP BY Email) AS KWAFIM_col2 ON Reservations.Email = KWAFIM_col2.Email
LEFT JOIN
(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') AND Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594) GROUP BY Email) AS GEL_col ON Reservations.Email = GEL_col.Email
LEFT JOIN
(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') GROUP BY Email) AS GEL_col2 ON Reservations.Email = GEL_col2.Email
Group by Full_Name, Manager, Region, Location, Reservations.Email;

#################################################################
########## Logstash-Mysql Reservations data import V1 ###########
#################################################################

SELECT
Reservations.*, VMUptime_Logs.Operation,
TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')) AS reserve_time,
Reservations.Start,
IF(Reservations.VMName LIKE CONCAT('%-Persistent-%'), 'Customized', 'On-Demand') AS lab_type,
Employees.Full_Name, Employees.Business_Title, Employees.Region, Employees.Location, Employees.Manager, Employees.Business_Unit, Employees.Department, Employees.ASE,
IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) AS modified
FROM
Reservations
LEFT JOIN Employees ON Reservations.Email = Employees.Email
LEFT JOIN  VMUptime_Logs ON Reservations.VMName = VMUptime_Logs.VMName
WHERE (IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) > FROM_UNIXTIME(1617718394) AND
IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) < UTC_TIMESTAMP())
ORDER BY modified;

#################################################################
########## Logstash-Mysql Reservations data import V2 ###########
#################################################################
# fixed bug: now consider only the 'Deleted' operations in VMUptime_logs
SELECT
Reservations.*,
TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')) AS reserve_time,
Reservations.Start,
IF(Reservations.VMName LIKE CONCAT('%-Persistent-%'), 'Customized', 'On-Demand') AS lab_type,
Employees.Full_Name, Employees.Business_Title, Employees.Region, Employees.Location, Employees.Manager, Employees.Business_Unit, Employees.Department, Employees.ASE,
IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) AS modified
FROM
Reservations
LEFT JOIN Employees ON Reservations.Email = Employees.Email
LEFT JOIN (SELECT * FROM VMUptime_Logs WHERE VMUptime_Logs.Operation = 'Deleted') AS VMUptime_Logs ON Reservations.VMName = VMUptime_Logs.VMName
WHERE (IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) > FROM_UNIXTIME(1617718394) AND
IF(VMUptime_Logs.Log IS NULL, TIMESTAMP(DATE_FORMAT(substring_index(substring_index(Reservations.VMName, ')-', -1),':', 3), '%Y-%m-%d %H:%i:%s')), VMUptime_Logs.Log) < UTC_TIMESTAMP())
ORDER BY modified;

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