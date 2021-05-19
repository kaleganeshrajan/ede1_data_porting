FROM  golang:1.15.7-buster

# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/github.com/brkelkar/ede1_data_porting

# Copy everything from the current directory to the PWD (Present Working Directory) inside the container
COPY . .

# Install the package
RUN go build -tags=jsoniter .


RUN  apt-get update -y
RUN  apt-get --assume-yes install -y python3
RUN  apt install --assume-yes  python3-pip
RUN pip3 install markdown-readme-generator
RUN  pip3 install -r ./file_convert/requirements.txt 

# Run the executable
CMD ["./ede1_data_porting"]