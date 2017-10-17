

#
# the default make target entry
#


default: 
	cd `pwd`/hw2.1_no_combiner/;mvn install
	cd `pwd`/hw2.2_combiner/;mvn install
	cd `pwd`/hw2.3_inmapper_combiner/;mvn install
	cd `pwd`/hw2.4_secondary_sort/;mvn install

local:
	cd `pwd`/hw2.1_no_combiner/;java -jar ./target/hw2.1-0.0.1-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/hw2.2_combiner/;java -jar ./target/hw2.2-0.0.1-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/hw2.3_inmapper_combiner/;java -jar ./target/hw2.3-0.0.1-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/hw2.4_secondary_sort/;java -jar ./target/hw2.4-0.0.1-jar-with-dependencies.jar ./Input ./output


# for secondary sort, put all .csv files inside the Input folder inside the 
# project directory.