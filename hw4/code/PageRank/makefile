

#
# the default make target entry
#


default: 
	sbt assembly
local:
	spark-submit ./target/scala-2.11/PageRank-assembly-1.0.jar ./input ./output


# for page rank input , put all .bz2 files in input folder