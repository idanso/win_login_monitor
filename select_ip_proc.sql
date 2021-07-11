CREATE DEFINER=`root`@`%` PROCEDURE `select_ip_proc`(IN ipAddress VARCHAR(20))
BEGIN
	SET @stmt_str =CONCAT("SELECT VMName FROM Reservations WHERE '",ipAddress,"' like CONCAT('%',RDP,'%') AND Reservations.Start <= UTC_TIME() AND Reservations.End >= UTC_TIME();");
	PREPARE stmt FROM @stmt_str;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;															
END