DELIMITER $$
CREATE DEFINER=`tribecaWebUser`@`%` PROCEDURE `sp_register`(myName varchar(254)
, given_name varchar(100)
, family_name varchar(100)
, email varchar(320)
, organization_name varchar(100)
, organization_phone varchar(254)
, organization_email varchar(320)
, experience int
, role varchar(254)
, why_participate varchar(254)
, what_tools varchar(254)
, out  loginAfterRegister bool
, out  errorCode int
, out  errorMessage varchar(1000))
body:BEGIN
	declare isAlreadyRegistered bool;
	declare isInWhiteList bool;
	declare userId bigint;

	set isAlreadyRegistered = false;
	set isInWhiteList = false;
	set loginAfterRegister = false;

	if exists(select *
			from user_info u
			where u.email = email and u.registered = 1)
	then
		set isAlreadyRegistered = true;
	end if;

	if exists(select *
			from white_list w
			where w.email = email)
	then
		set isInWhiteList = true;
	end if;

	if(isInWhiteList = true)
	then
		set loginAfterRegister = true;
	end if;

	if(isAlreadyRegistered = true)
	then
		if(isInWhiteList = true)
		then
			set errorCode = 1002;
			set errorMessage = 'The user has registered and is in the white list.';
		elseif(isInWhiteList = false)
		then
			set errorCode = 1003;
			set errorMessage = 'The user has registered and is in the waiting for approval.';
		end if;

		select u.uid as uid
		from user_info u
		where u.email = email;

		leave body;
	end if;
	


	if(isAlreadyRegistered = false and isInWhiteList = true)
	then
		select u.uid into userId
		from user_info u
		where u.email = email;

		UPDATE user_info
		SET
			given_name = given_name,
			family_name = family_name,
			registered = 1
		where uid = userId;
	elseif(isAlreadyRegistered = false and isInWhiteList = false)
	then
		INSERT INTO user_info
		(
			given_name,
			family_name,
			email,
			registered
		)
		VALUES
		(
			given_name,
			family_name,
			email,
			1
		);

		set userId = LAST_INSERT_ID();
	end if;

	select userId as uid;

	INSERT INTO user_registration
	(
		myName,
		uid,
		organization_name,
		organization_phone,
		organization_email,
		experience,
		role,
		why_participate,
		what_tools
	)
	VALUES
	(
		myName,
		userId,
		organization_name,
		organization_phone,
		organization_email,
		experience,
		role,
		why_participate,
		what_tools
	);


END$$
DELIMITER ;
