
/**************** Functions *******************/
function validate_form(form){
	
	jQuery.validator.setDefaults({
		debug: true,
		success: "valid"
	});

// Add US Phone Validation
jQuery.validator.addMethod('phoneUS', function(phone_number, element) {
    phone_number = phone_number.replace(/\s+/g, ''); 
    return this.optional(element) || phone_number.length > 9 &&
        phone_number.match(/^(1-?)?(\([2-9]\d{2}\)|[2-9]\d{2})-?[2-9]\d{2}-?\d{4}$/);
}, 'Please enter a valid phone number.');

	registrationForm = $( form ).validate({
		/*** here are the validation rules. the elements are get by name attribute ( <input type="field" name="firstname" /> ) ***/
		/********************HERE YOU CAN CUSTOMIZE THE STOCK VALIDATION RULES*******************/
		rules: {
			myname: {
				required: true,
				minlength: 3
			},
			companyname: {
				required: true,
				minlength: 3
			},
			role: {
				required: true,
			},
			experience: {
				required: true,
			},
			message1: {
				required: true,
				minlength: 3
			},
			message2: {
				required: true,
				minlength: 3
			},
			name: {
				required: true,
				minlength: 3
			},
			password:{
				required: true,
				minlength: 5
			},
			password2: {
			    required: true,
			    minlength: 5,
			    equalTo: "#password"
			},
			email: {
			    required: true,
			    minlength: 6,
				email: true
			}, 
			country: {
				required: true
			},
			gender: {
				required: true
			},
			terms:{
				required:true
			}, 
			url:{
				required:true, 
				url: true
			},
			rangelength:{
				required:true, 
				rangelength: [2, 6]
			},
			range:{
				required:true,
				range: [2,6]
			},
			phone_us:{
				required:true,
				phoneUS: true
			}, 
			number:{
				required:true,
				number:true
			}, 
			min:{
				required:true,
				min: 12
			},
			maxlength:{
				required:true,
				maxlength:4
			}, 
			nowhitespace:{
				required:true,
				nowhitespace: true
			}, 
			notEqual:{
				required:true,
				notEqual: "Your Name"
			},
			date:{
				required:true,
				date:true
			},
			letterswithbasicpunc:{
				required:true,
				letterswithbasicpunc: true
			},
			alphanumeric:{
				required:true,
				alphanumeric:true
			},
			lettersonly:{
				required:true,
				lettersonly:true
			}, 
			required:{
				required:true
			}

		},
		/******************** /HERE YOU CAN CUSTOMIZE THE STOCK VALIDATION RULES *******************/
		focusInvalid: false,
		onkeyup: false, 
		submitHandler: function(form) {
			var dataString = $(form).serialize();
            if($("#registerAuthResult").attr("value") == ""){
                handleAuthResultRegister()
            }
            else{
                form.submit()
            }
        },
		/***** Highlight effects on validation error *****/
		highlight: function(element, errorClass) {
			if(!$(element).is(':radio') && !$(element).is(':checkbox') && !$(element).is('select')){
	   			$(element).parent().find('span').removeClass().addClass('error');
				$(element).addClass(errorClass);
				/*** here you can change the color code if you want different border color when the field is not valid ***/
	    		$(element).parent().css('border','solid 1px #0071C5');
			}
			if($(element).is('select')){
				$(element).parent().find('.sbToggle').addClass('error');
				/*** here you can change the color code if you want different border color when the select box is not valid ***/
				$(element).parent().find('.sbHolder').css('border','solid 1px #0071C5');
			}
			if($(element).is(':checkbox')){
				/*** here you can change the color code if you want different border color when the checkbox is not valid ***/
				$(element).parent().find('.fa-icon-ok').css('border','solid 1px #0071C5');
			}
			if($(element).is(':radio')){
				/*** here you can change the color code if you want different border color when the radio button is not valid ***/
				$(element).parent().find('.fa-icon-circle').css('border','solid 1px #0071C5');
			}
		},
		/********** messages shown if error in validation happen ************/
		messages: {
		    myname: {
		        required: "Please enter your name",/*** message if the rule required is not fulfilled ***/
		        minlength: jQuery.format("Enter at least 3 characters"),/*** message if the rule minimum 3 characters is not fulfilled ***/
				notEqual: "Please specify a different (non-default) value" /*** message if the rule different from default value is not fulfilled ***/
			},
			
			password: {
		        required: "Please enter password",
		        minlength: jQuery.format("Password must be at least 5 characters long"),
				notEqual: "Please specify a different (non-default) value"
			},
			password2: {
		        required: "Please re-enter password",
		        equalTo: "Password fields have to match", /*** message if the two passwords matching rule is not fulfilled ***/
				notEqual: "Please specify a different (non-default) value" 
			},
			email:{
				required: "Please enter an e-mail",
				minlength: jQuery.format("E-mail must be at least 6 characters long"),
				email: "Please enter valid e-mail" 
			}, 
			country:{
				required:"Please select your country"
			},
			gender: {
				required: "Please select your gender"
			},
			terms: {
				required: "You must agree to the terms of use"
			}
		}	
	});

}	

function modalFade(){
	var formType = $(this).parent().attr('name');
	$('#black-screen').fadeIn('slow');
	$('.DA_custom_form').fadeIn();
	return false;
}
function call_func(){
	$("input[type='checkbox']").custCheckBox();
	$("input[type='radio']").custCheckBox();
	$(".DA_custom_form .select_field").selectbox();
	$('.DA_custom_form .sbToggle').append('<i class="fa-icon-chevron-down"></i>');
	$('.DA_custom_form .checkbox').append('<i class="fa-icon-ok"></i>');
	$('.DA_custom_form .radio').append('<i class="fa-icon-circle"></i>');
	$('.DA_custom_form .radio-btn, .DA_custom_form .check-box, .DA_custom_form .select_field').css('display','inline')
}

function all_events(){
		/*** checkbox behaviour ***/
		$('.DA_custom_form .checkbox').click(function(){

			if($(this).next('input').prop('checked') && $(this).parent().find('label').hasClass('error')){
				$(this).parent().find('label.error').addClass('hidden');
				/*** here you can change the border color of the checkbox when it's valid***/
				$(this).find('.fa-icon-ok').css('border','solid 1px #ccc');
				/*** /here you can change the border color of the checkbox when it's valid***/
			}else{
				$(this).parent().find('.hidden').removeClass('hidden');
			}
		});

		/*** radio button behaviour ***/
		$('.DA_custom_form .radio').click(function(){
			
			if($(this).next('input').prop('checked') && $(this).parent().find('label').hasClass('error')){
				$(this).parent().find('label.error').addClass('hidden');
				/*** here you can change the border color of the radio button when it's valid***/
				$(this).parent().find('.fa-icon-circle').css('border','solid 1px #ccc');
				/*** /here you can change the border color of the radio button when it's valid***/
			}else{
				$(this).parent().find('.hidden').removeClass('hidden');
			}
		});
		/*** text fields(textarea/input type="text") behaviour ***/
		$('.DA_custom_form .field').
		    focus(function() {
				if(!$(this).hasClass('error')){
						$(this).parent().animate({
							/*** here you can change the box shadow color of the field on focus  when it's not valid***/
							boxShadow: '0px 0px 10px 2px #0071C5'
						},90);
						/*** here you can change the border color of the field on focus when it's not valid***/
						$(this).parent().css('border','solid 1px #0071C5')
				}
				else{
					$(this).parent().animate({
						/*** here you can change the box shadow color of the field on focus when it's valid***/
						boxShadow: '0px 0px 10px 2px #0071C5'
					},90);
					/*** here you can change the border color of the field on focus when it's valid***/
					$(this).parent().css('border','solid 1px #0071C5')
				}
				if($(this).parent().find('span').hasClass('ok')){
					$(this).parent().find('span').removeClass();
					$(this).parent().find('i').removeClass('fa-icon-ok').addClass('fa-icon-remove');
				}
				
		    }).
		    blur(function(){
				if($(this).hasClass('valid') && this.value != '' ){
						$(this).parent().animate({
							/*** here you can change the box shadow color of the field on blur when it's valid***/
							boxShadow: '0px 0px 0px 0px #0071C5'},90);
						/*** here you can change the border color of the field on blur when it's valid***/
						$(this).parent().css('border','solid 1px #ccc');
						$(this).parent().find('i').removeClass('fa-icon-remove').addClass('fa-icon-ok');
						$(this).parent().find('span').removeClass('error').addClass('ok');
				}
				if($(this).hasClass('error')){
						$(this).parent().animate({
							/*** here you can change the box shadow color of the field on blur when it's not valid***/
							boxShadow: '0px 0px 0px 0px #0071C5'		
						},90);
						/*** here you can change the border color of the field on blur when it's not valid***/
						$(this).parent().css('border','solid 1px #0071C5');
						$(this).parent().find('i').removeClass('fa-icon-ok').addClass('fa-icon-remove');
						$(this).parent().find('span').removeClass('ok').addClass('error');
				}
				if(!$(this).hasClass('error') && this.value == '' ){
					
					$(this).parent().animate({
						/*** here you can change the box shadow color of the field on blur when it's empty ***/
						boxShadow: '0px 0px 0px 0px #0071C5'
					},90);
					/*** here you can change the border color of the field on blur when it's empty***/
					$(this).parent().css('border','solid 1px #ccc');
				}
		    });
		
		$('.DA_custom_form .sbOptions').click(function(){
			
			if($(this).parent().parent().find('select').val() != 0){
				$(this).parent().parent().find('label.error').hide();
				$(this).parent().find('.sbToggle').removeClass('error');
				$(this).parent().css('border','solid 1px #ccc')
			}
		});
		
		/*** close popup code. do the action when "close window" is clicked  ***/
		$('.close-window a').click(function(){
			$('#popup .DA_custom_form').fadeOut();
			$('#black-screen').fadeOut('slow');
			return false;
		});
		/*** /close popup code. do the action when "close window" is clicked  ***/
	}	
	/********** set no white space rule ************/
	jQuery.validator.addMethod("nowhitespace", function(value, element) {
        return this.optional(element) || /^\S+$/i.test(value);
	}, "No white space please");

	/********** Check if the value of the field is the default one ************/
	jQuery.validator.addMethod("notEqual", function(value, element, param) {
		  return this.optional(element) || value != param;
		}, "Please specify a different (non-default) value");

	/********** set use only letters and punctuations rule ************/
	jQuery.validator.addMethod("letterswithbasicpunc", function(value, element) {
	        return this.optional(element) || /^[a-z-.,()'\"\s]+$/i.test(value);
	}, "Letters or punctuation only please");  

	/********** set Letters, numbers, spaces or underscores only rule ************/
	jQuery.validator.addMethod("alphanumeric", function(value, element) {
	        return this.optional(element) || /^\w+$/i.test(value);
	}, "Letters, numbers, spaces or underscores only please");  

	/********** set use only letters rule ************/
	jQuery.validator.addMethod("lettersonly", function(value, element) {
	        return this.optional(element) || /^[a-z]+$/i.test(value);
	}, "Letters only please");



/* function that centers elements $('the element that has to be centered').center();*/
jQuery.fn.center = function(loaded) {
    var objs = this;
    body_width = parseInt($(window).width());
    objs.each(function() {
        var obj = $(this)
        var block_width = parseInt(obj.width());
        var left_position = parseInt((body_width/2) - (block_width/2)  + $(window).scrollLeft());
        if (body_width < block_width) { left_position = 0 };
        if(!loaded) {
            obj.css({'position': 'absolute'});
            obj.css({'left': left_position});
            obj.center(!loaded);
            $(window).bind('resize', function() { 
                obj.center(!loaded);
            });
            $(window).bind('scroll', function() { 
                obj.center(!loaded);
            });
        } else {
            obj.stop();
            obj.css({'position': 'absolute'});
            obj.animate({'left': left_position}, 200, 'linear');
        }
    });
}