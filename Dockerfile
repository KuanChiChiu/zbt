# This is a comment
FROM golang:dforcepro
MAINTAINER Docker Peter <ch.focke@gmail.com>





RUN mkdir /root/.ssh/
ADD id_rsa /root/.ssh
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan bitbucket.org >> /root/.ssh/known_hosts

RUN git clone 94peter@bitbucket.org:dforcepro/dforcepro.git /go/src/dforcepro.com

ADD . /go/src/zbt.dforcepro.com
RUN go install zbt.dforcepro.com

ENTRYPOINT /go/bin/zbt.dforcepro.com api /go/src/zbt.dforcepro.com/conf/

EXPOSE 9080
