FROM openjdk:11-jdk
WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN apt-get update && apt-get install -y maven

RUN mvn clean install

CMD ["tail", "-f", "/dev/null"]



