CREATE DEFINER=`root`@`%` PROCEDURE `insert_to_login_out_by_ip`(IN ipAddress VARCHAR(20),
IN tab_name VARCHAR(20))
BEGIN
	#DECLARE usr_name VARCHAR(35);
    DECLARE vm_name VARCHAR(100);
    #DECLARE tmp VARCHAR(100);
    IF ipAddress LIKE  '%10.248.241.32%' THEN
		SET vm_name = 'TAL TEST';
	ELSE
		SET vm_name = (SELECT DISTINCT VMName FROM Reservations
											WHERE ipAddress LIKE CONCAT('%',RDP,'%') 
											AND Reservations.Start <= UTC_TIME()
											AND Reservations.End >= UTC_TIME());
	END IF;								
	#SET usr_name = (SELECT substring_index(substring_index(tmp, '(', -1),')', 1));
	#SET vm_name = (SELECT substring_index(tmp,'-', 1));
                                                                
	SET @stmt_str =CONCAT('INSERT INTO ' , tab_name, ' VALUES ("', vm_name,'", NOW())');
	PREPARE stmt FROM @stmt_str;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;															
END