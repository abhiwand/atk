(function() {
  $(document).ready(function() {
    var touch;
    setValidateForm();
    setIEHelperClassses();
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * bootstrap carousel definition
    * --------------------------------------------------------------------------------------------------------------------
    */

    if (jQuery().carousel) {
      $('.carousel.carousel-auto').carousel();
      $('.carousel.carousel-auto').on("swipeleft", function(e) {
        return $(this).carousel('next');
      });
      $('.carousel.carousel-auto').on("swiperight", function(e) {
        return $(this).carousel('prev');
      });
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * circle statistics
    * --------------------------------------------------------------------------------------------------------------------
    */

    if (jQuery().knob) {
      $("[data-stat='circle']").each(function(i, el) {
        return $(el).knob();
      });
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * setting up bootstrap tooltips
    * --------------------------------------------------------------------------------------------------------------------
    */

    touch = false;
    if (window.Modernizr) {
      touch = Modernizr.touch;
    }
    if (!touch) {
      $("body").on("mouseenter", ".has-tooltip", function() {
        var el;
        el = $(this);
        if (el.data("tooltip") === undefined) {
          el.tooltip({
            placement: el.data("placement") || "top",
            container: "body"
          });
        }
        return el.tooltip("show");
      });
      $("body").on("mouseleave", ".has-tooltip", function() {
        return $(this).tooltip("hide");
      });
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * replacing *.svg images for *.png for browsers without *.svg support
    * --------------------------------------------------------------------------------------------------------------------
    */

    if (window.Modernizr && Modernizr.svg === false) {
      $("img[src*=\"svg\"]").attr("src", function() {
        return $(this).attr("src").replace(".svg", ".png");
      });
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * setting placeholders for browsers without placeholder support
    * --------------------------------------------------------------------------------------------------------------------
    */

    if (window.Modernizr && Modernizr.input.placeholder === false) {
      $("[placeholder]").focus(function() {
        var input;
        input = $(this);
        if (input.val() === input.attr("placeholder")) {
          input.val("");
          return input.removeClass("placeholder");
        }
      }).blur(function() {
        var input;
        input = $(this);
        if (input.val() === "" || input.val() === input.attr("placeholder")) {
          input.addClass("placeholder");
          return input.val(input.attr("placeholder"));
        }
      }).blur();
      $("[placeholder]").parents("form").submit(function() {
        return $(this).find("[placeholder]").each(function() {
          var input;
          input = $(this);
          if (input.val() === input.attr("placeholder")) {
            return input.val("");
          }
        });
      });
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * flexslider
    * --------------------------------------------------------------------------------------------------------------------
    */

    $(window).load(function() {
      var $allSlides, $flexslider;
      if (jQuery().flexslider) {
        $flexslider = $('.flexslider');
        $allSlides = $flexslider.find('.item');
        $flexslider.addClass("fade-loading");
        return $('.flexslider').flexslider({
          animation: 'fade',
          pauseOnHover: true,
          slideshowSpeed: 5000,
          animationSpeed: 400,
          prevText: '',
          nextText: '',
          before: function(slider) {
            var $activeSlide;
            $activeSlide = $flexslider.find('.flex-active-slide');
            if ($activeSlide.index() === $allSlides.length - 1) {
              $allSlides.eq(0).find('.animate').children().addClass("animate").removeClass("animated");
              $allSlides.not('.flex-active-slide').find('.animate').children().addClass("animate").removeClass("animated");
            } else {
              $allSlides.not('.flex-active-slide').find('.animate').children().addClass("animate").removeClass("animated");
            }
            return setTimeout((function() {
              return $allSlides.eq(slider.animatingTo).find('.animate').children().addClass("animated").removeClass("animate");
            }), 50);
          }
        });
      }
    });
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * setting up countdown plugin
    * --------------------------------------------------------------------------------------------------------------------
    */

    if (jQuery().countdown) {
      $("[data-countdown]").countdown();
    }
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * scroll top button
    * --------------------------------------------------------------------------------------------------------------------
    */

    $("#scroll-to-top").on("click", function(e) {
      $("body, html").animate({
        scrollTop: 0
      }, 800);
      return false;
    });
    return $(window).load(function() {
      var $scrollToTop, defaultBottom, scrollArea;
      $scrollToTop = $("#scroll-to-top");
      defaultBottom = $scrollToTop.css("bottom");
      scrollArea = function() {
        return $(document).outerHeight() - $("#footer").outerHeight() - $(window).outerHeight();
      };
      if ($('body').hasClass("boxed")) {
        return $(window).scroll(function() {
          if ($(this).scrollTop() > 500) {
            return $scrollToTop.addClass("in");
          } else {
            return $scrollToTop.removeClass("in");
          }
        });
      } else {
        return $(window).scroll(function() {
          if ($(this).scrollTop() > 500) {
            $scrollToTop.addClass("in");
          } else {
            $scrollToTop.removeClass("in");
          }
          if ($(this).scrollTop() >= scrollArea()) {
            return $scrollToTop.css({
              bottom: $(this).scrollTop() - scrollArea() + 10
            });
          } else {
            return $scrollToTop.css({
              bottom: defaultBottom
            });
          }
        });
      }
    });
  });

  /*
  * --------------------------------------------------------------------------------------------------------------------
  * form validation
  * --------------------------------------------------------------------------------------------------------------------
  */


  this.setValidateForm = function(selector) {
    if (selector == null) {
      selector = $(".form-validation");
    }
    if (jQuery().validate) {
      return selector.each(function(i, elem) {
        return $(elem).validate({
          errorElement: "span",
          errorClass: "help-block has-error",
          errorPlacement: function(err, e) {
            return e.closest('.control-group').append(err);
          },
          highlight: function(e) {
            return $(e).closest('.control-group').addClass('has-error');
          },
          unhighlight: function(e) {
            return $(e).closest('.control-group').removeClass('has-error');
          }
        });
      });
    }
  };

  /*
  * --------------------------------------------------------------------------------------------------------------------
  * internet explorer helpers classes :last-child, :nth-child
  * --------------------------------------------------------------------------------------------------------------------
  */


  this.setIEHelperClassses = function() {
    if (/msie/.test(navigator.userAgent.toLowerCase())) {
      $('*:last-child').addClass("last-child");
      $('*:nth-child(odd)').addClass("nth-child-odd");
      return $('*:nth-child(even)').addClass("nth-child-even");
    }
  };

}).call(this);
(function() {
  $(window).load(function() {
    /*
    * --------------------------------------------------------------------------------------------------------------------
    * isotope portfolio filtration
    * --------------------------------------------------------------------------------------------------------------------
    */

    var $portfolio;
    if (jQuery().isotope) {
      $portfolio = $("#portfolio-container");
      $portfolio.isotope({
        layoutMode: 'sloppyMasonry',
        itemSelector: ".portfolio-item"
      });
      return $("#portfolio-filter a").click(function() {
        var selector;
        $(this).closest("ul").find("li").removeClass("active");
        $(this).parent().addClass("active");
        selector = $(this).attr("data-filter");
        $portfolio.isotope({
          filter: selector
        });
        return false;
      });
    }
  });

}).call(this);


