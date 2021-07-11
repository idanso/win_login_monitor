CREATE DEFINER=`root`@`%` PROCEDURE `update_vmuptime_table`(IN vm varchar (256),
IN op int(2)
)
BEGIN

IF op = 0 THEN # case of power on
	INSERT INTO VMUptime VALUES (vm, NOW(), NULL);
    
ELSEIF op = 1 OR op = 2 THEN
	IF (SELECT PowerOff FROM VMUptime WHERE VMName = vm AND PowerOn = (SELECT MAX(PowerOn) FROM VMUptime WHERE VMName = vm)) IS NULL THEN
		SET @tmp_time = (SELECT MAX(PowerOn) FROM VMUptime WHERE VMName = vm);
		UPDATE VMUptime
		SET PowerOff = NOW()
		WHERE VMName = vm AND PowerOn = @tmp_time;
	ELSEIF op = 1 THEN
		INSERT INTO VMUptime VALUES (vm, NULL, NOW());
	END IF;
END IF;

END