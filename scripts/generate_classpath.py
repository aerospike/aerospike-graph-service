import os

def main():
    print("Generating classpath...")
    directory = "/opt/spark/jars"
    classpath_string = ""

    # Open jars directory and grab path to all jar files.
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            if classpath_string is "":
                classpath_string += f
            else:
                classpath_string += ":" + f

    # Write classpath to file.
    with open("/opt/classpath.txt", "w+") as classpath:
        classpath.write(classpath_string)

if __name__ == "__main__":
    main()
    