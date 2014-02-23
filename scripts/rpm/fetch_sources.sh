. ./versions.sh

rm -rf SOURCES

mkdir SOURCES

pushd SOURCES

project_name=intelanalytics
source_folder=$project_name-$TRIBECA_VERSION
python_folder=python-$source_folder
python_deps_precompiled_folder=$project_name-python-deps-precompiled-$TRIBECA_VERSION
python_deps_localbuild_folder=$project_name-python-deps-localbuild-$TRIBECA_VERSION
src=../../../IntelAnalytics
rm -rf $source_folder

mkdir $source_folder
mkdir $source_folder/target
shopt -s extglob
cp -R $src/!(exclude|dist|target|build|src|tests|*.ipynb|ipython|install_pyenv.sh) $source_folder
cp $src/target/*.jar $src/target/*.pig $source_folder/target/
cp $src/notebooks $source_folder/
rm $source_folder/original-*.jar
rm -rf $source_folder/intel_analytics
rm -rf $source_folder/bin/*python*
rm -rf $source_folder/conf/*python*
rm -rf $source_folder/conf/intel_analytics.properties


mkdir $python_folder
mkdir $python_folder/conf

cp -R $src/intel_analytics $python_folder
cp $src/conf/intel_analytics.properties $python_folder/conf
cp $src/conf/pig_log4j.properties $python_folder/conf
cp $src/conf/ipython_notebook_config.py $python_folder/conf
mkdir $python_folder/bin
cp -R $src/bin/IntelAnalytics-ipython $python_folder/bin

for f in $python_deps_precompiled_folder $python_deps_localbuild_folder
do
  mkdir $f

  cp $src/install_pyenv.sh $f
  tar -cvzf $f/template_overrides.tar.gz -C $src/ipython/TemplateOverrides .
  tar czvf $f.tar.gz $f
done

tar czvf $source_folder.tar.gz $source_folder
tar czvf $python_folder.tar.gz $python_folder

popd

