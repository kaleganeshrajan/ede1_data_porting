FROM  golang:1.15.13-alpine3.13
# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/github.com/kaleganeshrajan/ede1_data_porting

# Copy everything from the current directory to the PWD (Present Working Directory) inside the container
COPY . .

# Install the package
RUN go build -tags=jsoniter .

RUN apt-get update && apt-get -y install python3

 
RUN  apt install --assume-yes  python3-pip
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --user virtualenv
RUN pip3 install markdown-readme-generator
RUN  pip3 install -r ./file_convert/requirements.txt 


# Run the executable
CMD ["./ede_porting"]