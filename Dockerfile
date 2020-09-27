FROM hkarhani/ibfsta:latest
MAINTAINER Hassan El Karhani <hkarhani@gmail.com>

WORKDIR /notebooks

RUN cp -f requirements.txt /notebooks/requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /notebooks/settings
COPY *.py /notebooks
COPY settings/settings.yml /notebooks/settings/
RUN rm requirements.txt

EXPOSE 8888
CMD /bin/sh -c "/usr/bin/jupyter notebook --allow-root"
