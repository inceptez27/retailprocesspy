spark-build:
		rm -rf ./deploy
		mkdir -p ./deploy
		cp -f driver.py deploy
		cp -f app.properties deploy
		zip -r deploy/retail.zip retail