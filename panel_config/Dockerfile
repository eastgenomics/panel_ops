FROM ubuntu:focal-20220113

ARG panel_ops_version
ARG panel_palace_version
ARG generate_g2t_version

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/London

# install and download packages
WORKDIR /home
RUN apt-get update
RUN apt-get install -y wget build-essential zlib1g-dev python3-dev default-libmysqlclient-dev \
        build-essential python3-venv python3-pip curl jq libbz2-dev git liblzma-dev mysql-server tzdata libffi-dev
RUN wget https://www.python.org/ftp/python/3.9.1/Python-3.9.1.tgz
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN tar xzf Python-3.9.1.tgz

# setup Python
WORKDIR "/home/Python-3.9.1"
RUN ./configure --enable-optimizations
RUN make
RUN make install

RUN addgroup --gid 1000 panel_user
RUN adduser --disabled-password --gecos '' --uid 1000 --gid 1000 panel_user
USER panel_user

# create folders
RUN mkdir -p /home/panel_user/panels/panel_logs/
RUN mkdir -p /home/panel_user/panels/panel_ops/
RUN mkdir -p /home/panel_user/panels/panel_palace/
RUN mkdir -p /home/panel_user/panels/generate_g2t/

# download and setup panel ops
WORKDIR /home/panel_user/panels/
RUN git clone https://github.com/eastgenomics/panel_ops
WORKDIR /home/panel_user/panels/panel_ops/
RUN git pull
RUN git checkout ${panel_ops_version}

# RUN git clone https://github.com/eastgenomics/panel_ops
# WORKDIR /home/panel_user/panels/panel_ops/
# RUN git checkout ${panel_ops_version} 

WORKDIR /home/panel_user/panels/
# setup panel environment
RUN pip3 install -r panel_ops/requirements.txt

# setup generate g2t
RUN wget https://github.com/eastgenomics/generate_g2t/archive/refs/tags/${generate_g2t_version}.tar.gz
RUN tar xzf ${generate_g2t_version}.tar.gz -C generate_g2t
RUN mv generate_g2t/* generate_g2t/${generate_g2t_version}

# setup panel palace
# RUN git clone https://github.com/eastgenomics/panel_palace
# WORKDIR /home/panel_user/panels/panel_palace/
# RUN git checkout ${panel_palace_version}

RUN git clone https://github.com/eastgenomics/panel_palace
WORKDIR /home/panel_user/panels/panel_palace/
RUN git pull
RUN git checkout ${panel_palace_version}

WORKDIR /home/panel_user/panels