cd /apps/staging/Inceptezworks/retailprocesspy
git pull origin master

make

rm -rf /apps/projects/Inceptezworks/*
cp -rf /apps/staging/Inceptezworks/retailprocesspy/deploy /apps/projects/Inceptezworks/
cp -rf /apps/staging/Inceptezworks/retailprocesspy/app.properties /apps/projects/Inceptezworks/
cp -rf /apps/staging/Inceptezworks/retailprocesspy/driver.py /apps/projects/Inceptezworks/

cd /apps/projects/Inceptezworks/

spark-submit --master local[*] --py-files file:/apps/projects/Inceptezworks/deploy/retail.zip driver.py