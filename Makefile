spark-build:
		rm -rf ./deploy
		mkdir -p ./deploy
		cp driver.py deploy
		zip -r deploy/retail.zip retail