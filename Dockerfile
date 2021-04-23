FROM python:3.6.9-slim-stretch

RUN apt-get update && \
    apt-get install -y vim
    
RUN apt-get install -y curl wget

RUN curl https://sdk.cloud.google.com | bash && \
    echo '. /root/google-cloud-sdk/completion.bash.inc' >> ~/.bashrc && \
    echo '. /root/google-cloud-sdk/path.bash.inc' >> ~/.bashrc

ENV PATH $PATH:/root/google-cloud-sdk/bin

# re: mkdir, https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199#23

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && apt-get install -y \
    openjdk-8-jre-headless \
    && rm -rf /var/lib/apt/lists/* && \
    pip3 --no-cache-dir install hail==0.2.61

# need the *.sh, *.py files, *.yaml files only -- the *.tsv should be a workflow input
COPY * /

ENTRYPOINT []
