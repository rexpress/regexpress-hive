FROM regexpress/maven:oraclejdk8

ENV REPOSITORY=regexpress-hive
ENV ARTIFACT=hive
ENV BRANCH=2.10

RUN cd /tmp && \
    wget https://github.com/rexpress/regexpress-common/archive/master.zip && \
    unzip master.zip && \
    mvn -f regexpress-common-master install && \
    rm -rf master.zip && \
    wget https://github.com/rexpress/$REPOSITORY/archive/$BRANCH.zip && \
    unzip $BRANCH.zip && \
    mvn -f $REPOSITORY-$BRANCH package && \
    rm -rf $BRANCH.zip && \
    mv $REPOSITORY-$BRANCH/target/lib /root && \
    mv $REPOSITORY-$BRANCH/target/ARTIFACT-$BRANCH.jar /root && \
    rm -rf /tmp/* && \
    echo 'java -jar /root/ARTIFACT-$BRANCH.jar "$1" "$2"' > /root/run.sh && \
    chmod 755 /root/run.sh

ENTRYPOINT ["sh","/root/run.sh"]