DELIMITER $$
CREATE DEFINER=`tribecaWebUser`@`%` PROCEDURE `sp_login`(email varchar(320), out loginSuccessful bool, out errorCode int, out errorMessage varchar(1000))
body:BEGIN
	
	set loginSuccessful = false;

	if not exists(select *
			from user_info u
			where u.email = email)
	then
		set errorCode = 1004;
		set errorMessage = 'Please register first.';
		leave body;
	end if;

	select u.uid as uid
	from user_info u
	where u.email = email;

	if not exists(select *
			from white_list w
			where w.email = email)
	then
		set errorCode = 1003;
		set errorMessage = 'We are currently processing your registration request.';
		leave body;
	end if;

	set loginSuccessful = true;
	
END$$
DELIMITER ;
