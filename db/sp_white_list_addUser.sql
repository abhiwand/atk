DELIMITER $$
CREATE DEFINER=`tribecaWebUser`@`%` PROCEDURE `sp_white_list_addUser`(email varchar(320), out errorCode int, out errorMessage varchar(1000))
body:BEGIN
	declare uid bigint;

	declare EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
			select 'rollback';
          ROLLBACK;
    END;

	set email = trim(email);
	set errorCode = 0;

	if exists(select *
		from white_list w
		where w.email = email)
	then
		set errorCode = 1001;
		set errorMessage = "The email is already in the white list";
		leave body;
	end if;

	START TRANSACTION;
	insert into user_info (email, registered) values (email, 0);
	set uid = LAST_INSERT_ID();
	insert into white_list (uid, email) values (uid, email);
	COMMIT;
END$$
DELIMITER ;
