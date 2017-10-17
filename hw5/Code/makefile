

#
# the default make target entry
#


default: 
	cd `pwd`/rxc/;mvn install
	cd `pwd`/cxr/;mvn install

local:
	cd `pwd`/rxc/;java -jar ./target/hw5.1-0.0.1-jar-with-dependencies.jar ./input ./output
	cd `pwd`/cxr/;java -jar ./target/hw5.2-0.0.1-jar-with-dependencies.jar ./input ./output
