cd ../../tribeca                 
if [ ! -d dist ]                 
  then mkdir dist                
fi                               
virtualenv dist/ENV              
source dist/ENV/bin/activate     
python setup.py install          
pip install numpy                
pip install scipy                
pip install matplotlib           
pip install ipython              
pip install pyzmq                
pip install jinja2               
pip install tornado              
deactivate                       
virtualenv --relocatable dist/ENV

