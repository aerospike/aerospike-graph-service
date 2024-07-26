import csv, sys, os


try:
    os.makedirs('src/test/resources/recoverydata/vertices', exist_ok=True)
    with open('src/test/resources/recoverydata/vertices/vertexList.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        # Loop 10k times
        writer.writerow(['~id', 'label'])
        for i in range(2500000):
            writer.writerow([i, 'vertex'])
    os.makedirs('src/test/resources/recoverydata/edges', exist_ok=True)
    with open('src/test/resources/recoverydata/edges/edgeList.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        # Loop 10k times
        writer.writerow(['~id', 'label', '~from', '~to'])
        for i in range(1000000):
            writer.writerow([i, 'edge', i, 1])
        for i in range(2499999):
            writer.writerow([i + 1000000, 'edge', i, i + 1])
    print("Success")
    sys.exit(0)
except Exception as e:
    sys.exit(1)