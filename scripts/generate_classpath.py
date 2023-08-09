import os

def main():
    print("Generating classpath...")
    spark_directory = "/opt/spark/jars"
    bulk_loader_directory = "/opt/bulk-loader"
    classpath_string = ""

    # Open spark jars directory and grab path to all jar files.
    for filename in os.listdir(spark_directory):
        f = os.path.join(spark_directory, filename)
        if os.path.isfile(f):
            if classpath_string is "":
                classpath_string += f
            else:
                classpath_string += ":" + f

    # Open bulk loader jars directory and grab path to all jar files.
    for filename in os.listdir(bulk_loader_directory):
        f = os.path.join(bulk_loader_directory, filename)
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
    