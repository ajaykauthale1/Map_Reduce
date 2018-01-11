spark.root=/home/spark-2.2.0
jar.name=sparkpagerank-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
job.name=sparkpagerank.SparkPageRank
local.input=input
local.output=output

# AWS EMR Execution
aws.emr.release=emr-5.4.0
aws.region=us-west-2
aws.bucket.name=cs6240-ajay-map-reduce-pagerank-spark
aws.subnet.id=subnet-43a73b1b
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

alone: jar
	${spark.root}/bin/spark-submit --class ${job.name} ${jar.path}  ${local.input} ${local.output}

make-bucket:
	aws s3 mb s3://${aws.bucket.name}

upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws
	aws emr create-cluster \
		--name "Page Rank Spark Cluster - 10 Machines" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master","yarn","--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
		--applications Name=Spark \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate