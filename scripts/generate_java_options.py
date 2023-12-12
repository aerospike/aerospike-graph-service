import os


def main():
    print("Generating java options...")
    java_options = ""
    omit_xmx = False

    if os.environ.get("JAVA_OPTIONS") is not None:
        user_java_options = os.environ.get("JAVA_OPTIONS")
        if "-xmx" in user_java_options.lower():
            omit_xmx = True
        java_options += user_java_options
    if not omit_xmx:
        mem_mib = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024. ** 2)
        max_memory = int(mem_mib * 0.8)  # 80% of system memory
        java_options += f" -Xmx{max_memory}m"

    java_options += " --add-exports java.base/sun.nio.ch=ALL-UNNAMED "

    # Write classpath to file. Use 'w' to overwrite file.
    with open("/opt/scripts/java_options.txt", "w") as java_options_file:
        java_options_file.write(java_options)


if __name__ == "__main__":
    main()
