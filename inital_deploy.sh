#check and Install git
GIT_VERSION="$(git --version)"
if [ "$GIT_VERSION" != "command not found" ]; then
   echo "git installed" 
else
    echo "git is missing and installing it"
    sudo apt-get update
    sudo apt install git
    git config --global user.name "srini"
	git config --global user.email "kgs.sri@gmail.com"
fi


#copy the gcs files into hdfs
gsutil ls gs://inceptezbatch27/
rm -rf /tmp/retaildata
mkdir -p /tmp/retaildata
gsutil cp gs://inceptezbatch27/RetailData/* /tmp/retaildata/

#copy data from local fs to hdfs
hadoop fs -mkdir -p /tmp/retaildata
hadoop fs -put /tmp/retaildata/* /tmp/retaildata/
hadoop fs -ls /tmp/retaildata/

#Create staging,archive and production folders
sudo mkdir -p /apps/staging/Inceptezworks
sudo mkdir -p /apps/archive/Inceptezworks
sudo mkdir -p /apps/projects/Inceptezworks

sudo chown -R gcpuser:gcpuser /apps/staging
sudo chown -R gcpuser:gcpuser /apps/archive
sudo chown -R gcpuser:gcpuser /apps/projects

cd /apps/staging/Inceptezworks/retailprocesspy

make

#Copy the files
rm -rf /apps/projects/Inceptezworks/*
cp -rf /apps/staging/Inceptezworks/retailprocesspy/deploy /apps/projects/Inceptezworks/
cp -rf /apps/staging/Inceptezworks/retailprocesspy/app.properties /apps/projects/Inceptezworks/
cp -rf /apps/staging/Inceptezworks/retailprocesspy/driver.py /apps/projects/Inceptezworks/

cd /apps/projects/Inceptezworks/

spark-submit --master local[*] --py-files file:/apps/projects/Inceptezworks/deploy/retail.zip driver.py
