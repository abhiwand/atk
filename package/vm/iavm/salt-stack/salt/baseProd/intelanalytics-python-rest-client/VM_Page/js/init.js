//------------------------------init jpreLoader-----------------------
$('#wrapper').jpreLoader({
	loaderVPos: '50%',
	autoClose: true,
	}, 
	function() {
		$('#wrapper').animate({"opacity":'1'},{queue:false,duration:700,easing:"easeInOutQuad"});
		setTimeout(function () {
		var ww = $(window).width();		
		if(ww < 959){					
				$('.counter-holder').animate({'opacity':'1' , top:'5%'},870);			
		}
		if(ww > 959){					
				$('.counter-holder').animate({'opacity':'1' , top:'10%'},870);			
		}			
		var prh = $('.progress-holder');
		var prs = $('.progress span');
		setTimeout(function () {
			var animationOn = jQuery.parseJSON((prh.attr("data-animate-progress")));
			prh.find('.progress').animate(animationOn, {queue:false, duration:1200});
		
			setTimeout(function () {
				prs.addClass('visprogress');
			}, 1250);
			setTimeout(function () {
				prs.removeClass('visprogress');
			}, 3000);
		}, 1050);	
	}, 850);
});	
function initChudo() {
	
	// Author code here

	"use strict";
	
// init countdown------------------------------------------------------

	$('.countdown').downCount({
		date: '09/12/2015 12:00:00', // your date
		offset: 0
	});	
		
// FitText-------------------------------------------------------------
	
	$('h2').fitText(1.8,{minFontSize:'30px',maxFontSize:'32px'});
		
// NiceScroll----------------------------------------------------------

	$("html").niceScroll({
		zindex:1000,
	  	cursorborderradius:0, 
		cursorborder:false,
		scrollspeed: 90,
		mousescrollstep: 40, 
		horizrailenabled:false
	});
	
// Content   animation ----------------------------------------	

	function showgrid(){
		$('.counter-holder').animate({'opacity':'0' , top:'-2%'},870,function(){
			$('.counter-holder').fadeOut(10);
		});
		$('.info-decor-holder').fadeIn(10);
		setTimeout(function () {
				$('.info-decor').each(function (index) {
    			var $this = $(this);
    			setTimeout(function () {
        			$this.addClass('scale-callback');
    			}, 250 * index );
			});
    	}, 950);			
	}
	
	function hidegrid(){	
		$('.info-decor').each(function (index) {
		var $this = $(this);
		setTimeout(function () {
			$this.removeClass('scale-callback');
		}, 250 * index );
		setTimeout(function () {
			$('.counter-holder').fadeIn(10,function(){
				var ww = $(window).width();		
				if(ww < 959){					
						$('.counter-holder').animate({'opacity':'1' , top:'5%'},{queue:false,duration:700});			
				}
				if(ww > 959){					
						$('.counter-holder').animate({'opacity':'1' , top:'10%'},{queue:false,duration:700});		
				}
			});	
			$('.info-decor-holder').fadeOut(10);
		}, 1350);
		});		
	}
	
	$('.show-grid').click(function(){
			showgrid();
		    setTimeout(function () {
			$('.fade-content').fadeIn(100);
			$('.vis').animate({'opacity':'1' , left:0},1000);
    	}, 2550);	
	});
	
	$('.hide-grid').click(function(event){
		event.preventDefault();
		$('.fade-content').fadeOut(100);
		$('.vis').animate({'opacity':'0' , left:'25%'},1000);
		setTimeout(function () {
			hidegrid();
   		 }, 650);
	});

	// Content    ----------------------------------------

	$('#content-slider li.slide').hide();
	$('#content-slider li.slide:first').show();
	$('#content-slider li.slide:first').addClass('acttab');
	$('.info-holder a.slider-link').click(function(){
		$('.info-holder a.slider-link').removeClass('activeslide');
		$(this).addClass('activeslide');	
		$('#content-slider li.slide').removeClass('acttab');
		var currentTab=$(this).attr('href');
		$('#content-slider li.slide').hide(1,function(){
			$('.container').animate({'opacity':'0' , left:'25%'},1);
			
			});
		$(currentTab).show(1,function(){
			$('.container').animate({'opacity':'1' , left:0},1000);
			
			});
		return false;
	});
	
	// owlCarousel   ----------------------------------------
		
	$("#photo-slider").owlCarousel({		   
		navigation : true,
		pagination:false, 
		slideSpeed : 300,
		paginationSpeed : 400,
		singleItem:true,
		transitionStyle : "goDown"			  
	});
		
	$("#text-rotator").owlCarousel({
		navigation : false,
		pagination:false,
		singleItem : true,
		autoHeight : false,
		mouseDrag:	false,	
		touchDrag:false,
		autoPlay:3500,		
     });

	// Twitter   ----------------------------------------
		
	if ($('#twitter-feed').length) {
		$('#twitter-feed').tweet({
			username: 'katokli3mmm',
			join_text: 'auto',
			avatar_size: 0,
			count: 5
		});
			
		$('#twitter-feed').find('ul').addClass('twitter-slider');
		$('#twitter-feed').find('ul li').addClass('item');
		var owl = $(".twitter-slider");
			owl.owlCarousel({
				navigation:false,
				slideSpeed : 500,
				pagination :false,
				autoHeight : true,
				singleItem:true,
				transitionStyle : "goDown"
		});	
		$(".next-slide").click(function(){
			owl.trigger('owl.next');		
		});
		$(".prev-slide").click(function(){
			owl.trigger('owl.prev');		
		});
	};			
		
	// Contact form   ----------------------------------------

	$('#contactform').submit(function(){
		var action = $(this).attr('action');
		$("#message").slideUp(750,function() {
		$('#message').slideUp(500);
 		$('#submit').attr('disabled','disabled');
		$.post(action, {
			name: $('#name').val(),
			email: $('#email').val(),
			comments: $('#comments').val()
		},
			function(data){
				document.getElementById('message').innerHTML = data;
				$('#message').slideDown('slow');
				$('#submit').removeAttr('disabled');
			}
		);
		});
		return false;
	});
	$("#contactform input, #contactform textarea").keyup(function(){		
		$("#message").slideUp(1500)			
	});	
	
	// Subscribe   ----------------------------------------

	$('.subscriptionForm').submit(function(){		
		var email = $('#subscriptionForm').val();
		$.ajax({
			url:'php/subscription.php',
			type :'POST',
			dataType:'json',
			data: {'email': email},success: function(data){
				if(data.error){
					$('#error').fadeIn()
				}
				else{
					$('#success').fadeIn();
					$("#error").hide();}
				}
			});
		return false
	});
	
	$('#subscriptionForm').focus(function(){
		$('#error').fadeOut();
		$('#success').fadeOut();	
	});
	
	$('#subscriptionForm').keydown(function(){	
		$('#error').fadeOut();
		$('#success').fadeOut();		
	});		
	
}

$(document).ready(function(){
	initChudo();
});