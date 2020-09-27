FROM hkarhani/ibfsta:latest
MAINTAINER Hassan El Karhani <hkarhani@gmail.com>

RUN pip install -r requirements.txt



RUN mkdir /notebooks/settings
COPY ./*.py /notebooks
COPY ./settings/settings.yml /notebooks/settings/
RUN rm requirements.txt

WORKDIR /notebooks

EXPOSE 8888
CMD /bin/sh -c "/usr/bin/jupyter notebook --allow-root"
