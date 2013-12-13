. ./versions.sh

rm -rf SOURCES

mkdir SOURCES

pushd SOURCES

project_name=intelanalytics
source_folder=$project_name-$TRIBECA_VERSION
python_folder=python-$source_folder
python_dependencies_folder=python-$project_name-dependencies-$TRIBECA_VERSION
src=../../../IntelAnalytics
rm -rf $source_folder

mkdir $source_folder
shopt -s extglob
cp -R $src/!(exclude|dist|target|build|src|tests|*.ipynb|ipython|install_pyenv.sh) $source_folder
cp $src/target/*.jar $source_folder
rm -rf $source_folder/intel_analytics
rm -rf $source_folder/bin/*python*
rm -rf $source_folder/conf/*python*
rm -rf $source_folder/conf/intel_analytics.properties


mkdir $python_folder
mkdir $python_folder/conf

cp -R $src/intel_analytics $python_folder
cp $src/conf/intel_analytics.properties $python_folder/conf
cp $src/conf/ipython_notebook_config.py $python_folder/conf
mkdir $python_folder/bin
cp -R $src/bin/IntelAnalytics-ipython $python_folder/bin

mkdir $python_dependencies_folder

cp $src/install_pyenv.sh $python_dependencies_folder

tar czvf $source_folder.tar.gz $source_folder
tar czvf $python_folder.tar.gz $python_folder
tar czvf $python_dependencies_folder.tar.gz $python_dependencies_folder
popd

