####################################################
#输出评分图
####################################################
--install.packages("ggplot2") #安装ggplot2
library(ggplot2)
evaluator<-read.table(file="evaluator.csv",header=TRUE,sep=",")
g<-ggplot(data=evaluator,aes(precision,recall,col=algorithm))
g<-g+geom_point(aes(size=3))
g<-g+geom_text(aes(label=algorithm))
g<-g+scale_x_continuous(limits=c(0,1))+scale_y_continuous(limits=c(0,1))
g


job<-read.table(file="job.csv",header=FALSE,sep=",")
names(job)<-c("jobid","create_date","salary")

pv<-read.table(file="pv.csv",header=FALSE,sep=",")
names(pv)<-c("userid","jobid")

pv[which(pv$userid==974),]
j1<-job[job$jobid %in% c(106,173,82,188,78),];j1
job[job$jobid %in% c(145,121,98,19),]
job[job$jobid %in% c(145,89,19),]

avg08<-mean(j1$salary)*0.8;avg08
