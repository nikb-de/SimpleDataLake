FROM bde2020/spark-submit:3.1.1-hadoop3.2

LABEL Name=pysparkExample 

# Updating package manager
COPY ./pysparkJobs/template.sh /
# installing packages needed and pip packages
RUN python3 -m pip install pyspark

COPY ./pysparkJobs/ /app
# running the python file at container startup
CMD ["/bin/bash", "/template.sh"]
