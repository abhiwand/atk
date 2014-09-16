Name: intelanalytics-python-rest-client
Summary: intelanalytics-python-rest-client-0.8 Build number: 901. TimeStamp 20140624195859Z
License: Confidential
Version: 0.8
Group: Intel Analytics
Requires: python2.7 python2.7-pip python2.7-pandas
Release: 901
Source: intelanalytics-python-rest-client-0.8.tar.gz
URL: graphtrial.intel.com
%description
intelanalytics-python-rest-client-0.8 Build number: 901. TimeStamp 20140624195859Z
commit 06aee68eee188e14b34c781b828307450130f5d0 Merge: 5db0b28 2165283 Author: rodorad <rene.o.dorado@intel.com> Date: Tue Jun 24 12:04:01 2014 -0700 Merge remote-tracking branch 'origin/package' into sprint_14_package
%define TIMESTAMP %(echo 20140624195859Z)
%define TAR_FILE %(echo /home/rodorad/IdeaProjects/intellij/source_code/package/intelanalytics-python-rest-client-source.tar.gz)
%build
 cp %{TAR_FILE} %{_builddir}/files.tar.gz
%install
 rm -rf %{buildroot}
 mkdir -p %{buildroot}
 mv files.tar.gz %{buildroot}/files.tar.gz
 tar -xvf %{buildroot}/files.tar.gz -C %{buildroot}
 rm %{buildroot}/files.tar.gz
%clean
%pre
%post

 #sim link to python sites packages
 if [ -d /usr/lib/python2.7/site-packages/intelanalytics ]; then
   rm /usr/lib/python2.7/site-packages/intelanalytics
 fi

 ln -s /usr/lib/intelanalytics/rest-client/python  /usr/lib/python2.7/site-packages/intelanalytics

 #run requirements file
 pip2.7 install -r /usr/lib/intelanalytics/rest-client/python/requirements.txt

%preun
%postun

 if  [ $1 -eq 0 ]; then
    rm /usr/lib/python2.7/site-packages/intelanalytics
 fi

%files

/usr/lib/intelanalytics/rest-client

/usr/lib/intelanalytics/rest-client/python/rest/hooks.py
/usr/lib/intelanalytics/rest-client/python/rest/message.py
/usr/lib/intelanalytics/rest-client/python/rest/cloudpickle.py
/usr/lib/intelanalytics/rest-client/python/rest/graph.py
/usr/lib/intelanalytics/rest-client/python/rest/serializers.py
/usr/lib/intelanalytics/rest-client/python/rest/spark.py
/usr/lib/intelanalytics/rest-client/python/rest/frame.py
/usr/lib/intelanalytics/rest-client/python/rest/connection.py
/usr/lib/intelanalytics/rest-client/python/rest/prettytable.py
/usr/lib/intelanalytics/rest-client/python/rest/launcher.py
/usr/lib/intelanalytics/rest-client/python/rest/command.py
/usr/lib/intelanalytics/rest-client/python/rest/__init__.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_row.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/test_little_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_api.py
/usr/lib/intelanalytics/rest-client/python/tests/test_serialize.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_connection.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_frame.py
/usr/lib/intelanalytics/rest-client/python/tests/iatest.py
/usr/lib/intelanalytics/rest-client/python/tests/test_rest_graph.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_types.py
/usr/lib/intelanalytics/rest-client/python/tests/sources.py
/usr/lib/intelanalytics/rest-client/python/tests/.gitignore
/usr/lib/intelanalytics/rest-client/python/tests/test_webhook.py
/usr/lib/intelanalytics/rest-client/python/tests/test_tmp_gb_json.py
/usr/lib/intelanalytics/rest-client/python/tests/test_sources.py
/usr/lib/intelanalytics/rest-client/python/tests/test_core_files.py
/usr/lib/intelanalytics/rest-client/python/tests/run_doctests.sh
/usr/lib/intelanalytics/rest-client/python/tests/exec_all.sh
/usr/lib/intelanalytics/rest-client/python/tests/__init__.py
/usr/lib/intelanalytics/rest-client/python/requirements.txt
/usr/lib/intelanalytics/rest-client/python/little/frame.py
/usr/lib/intelanalytics/rest-client/python/little/__init__.py
/usr/lib/intelanalytics/rest-client/python/doc/make_html.sh
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_6.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_2b.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_lbp_1.png
/usr/lib/intelanalytics/rest-client/python/doc/source/rowfunc.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_4.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_yum.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_als_2.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_als_3.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_apio.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_lda_1.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_samp.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/index.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_apic.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_5.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_2a.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_apim.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_apt.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_trbl.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_als_1.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_db.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ia_legl.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_ml.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ia_intr.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/_static/strike.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_static/intel-logo.jpg
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_inst.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/examples.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_conf.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_over.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_misc.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_lp_1.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ia_glos.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/js/copybutton.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/scipy.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/css/spc-extend.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/css/scipy-central.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/css/extend.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/css/pygments.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/css/spc-bootstrap.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/grid.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/dropdowns.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/layouts.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/type.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/forms.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/navs.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/carousel.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/tables.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/variables.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/thumbnails.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive-navbar.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/bootstrap.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/wells.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/media.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive-767px-max.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/close.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/scaffolding.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/button-groups.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/popovers.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/utilities.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/hero-unit.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/reset.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/code.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/alerts.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/pager.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/pagination.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive-utilities.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/navbar.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/mixins.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/modals.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/component-animations.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/sprites.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/breadcrumbs.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/tooltip.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive-1200px-min.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/accordion.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/progress-bars.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/responsive-768px-979px.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/buttons.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/bootstrap/labels-badges.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-footer.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-header.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-utils.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-extend.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-rightsidebar.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-bootstrap.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/static/less/spc-content.less
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/sourcelink.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/was_searchbox.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scipy/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/traditional/static/traditional.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/traditional/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/sphinxdoc/static/contents.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/sphinxdoc/static/navigation.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/sphinxdoc/static/sphinxdoc.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/sphinxdoc/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/sphinxdoc/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/epub/static/epub.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/epub/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/epub/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/epub/epub-cover.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/file.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/comment.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/down-pressed.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/minus.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/jquery.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/doctools.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/up.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/underscore.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/searchtools.js_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/ajax-loader.gif
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/up-pressed.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/comment-close.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/websupport.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/plus.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/comment-bright.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/basic.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/static/down.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/localtoc.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/genindex-split.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/genindex-single.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/globaltoc.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/opensearch.xml
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/search.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/genindex.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/sourcelink.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/searchbox.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/domainindex.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/defindex.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/page.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/searchresults.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/changes/versionchanges.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/changes/rstsource.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/changes/frameset.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/basic/relations.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/nature/static/nature.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/nature/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/default/static/default.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/default/static/sidebar.js_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/default/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/default/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/static/bg-page.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/static/alert_info_32.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/static/aldrich.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/static/bullet_orange.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/static/alert_warning_32.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/aldrich/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/agogo/static/bgtop.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/agogo/static/agogo.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/agogo/static/bgfooter.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/agogo/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/agogo/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/metal.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/scrolls.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/logo.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/darkmetal.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/print.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/theme_extras.js
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/navigation.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/watermark_blur.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/watermark.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/static/headerbg.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/scrolls/artwork/logo.svg
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/transparent.gif
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/dialog-todo.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/dialog-topic.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/dialog-warning.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/footerbg.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/dialog-seealso.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/ie6.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/dialog-note.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/pyramid.css_t
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/middlebg.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/headerbg.png
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/static/epub.css
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/theme.conf
/usr/lib/intelanalytics/rest-client/python/doc/source/_theme/pyramid/layout.html
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_reqs.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_dflw.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_7.png
/usr/lib/intelanalytics/rest-client/python/doc/source/intel_legal.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_over.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_apir.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/conf.py
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_1.png
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_strt.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ad_mntc.rst
/usr/lib/intelanalytics/rest-client/python/doc/source/ds_mlal_fig_3.png
/usr/lib/intelanalytics/rest-client/python/doc/map
/usr/lib/intelanalytics/rest-client/python/doc/git.readme.txt
/usr/lib/intelanalytics/rest-client/python/doc/techwriters
/usr/lib/intelanalytics/rest-client/python/doc/install_packages.sh
/usr/lib/intelanalytics/rest-client/python/doc/raw_data.csv
/usr/lib/intelanalytics/rest-client/python/doc/Makefile
/usr/lib/intelanalytics/rest-client/python/doc/toctreeWarnings
/usr/lib/intelanalytics/rest-client/python/doc/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/files.py
/usr/lib/intelanalytics/rest-client/python/core/aggregation.py
/usr/lib/intelanalytics/rest-client/python/core/serialize.py
/usr/lib/intelanalytics/rest-client/python/core/graph.py
/usr/lib/intelanalytics/rest-client/python/core/backend.py
/usr/lib/intelanalytics/rest-client/python/core/loggers.py
/usr/lib/intelanalytics/rest-client/python/core/column.py
/usr/lib/intelanalytics/rest-client/python/core/config.py
/usr/lib/intelanalytics/rest-client/python/core/errorhandle.py
/usr/lib/intelanalytics/rest-client/python/core/frame.py
/usr/lib/intelanalytics/rest-client/python/core/row.py
/usr/lib/intelanalytics/rest-client/python/core/__init__.py
/usr/lib/intelanalytics/rest-client/python/core/types.py
/usr/lib/intelanalytics/rest-client/python/__init__.py
