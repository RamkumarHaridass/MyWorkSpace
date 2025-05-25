import xml.etree.ElementTree as ET
import argparse
import csv

def sortElements(element):
    element[:] = sorted(element, key=lambda x: (x.tag, sorted(x.attrib.items())))
    for child in element:
        sortElements(child)

def compareElements(e1, e2, path=""):
    differences = []
    if e1.tag != e2.tag:
        differences.append([path, "Tag", e1.tag, e2.tag])
    if e1.text != e2.text:
        differences.append([f"{path}/{e1.tag}", "Text", e1.text, e2.text])
    if e1.tail != e2.tail:
        differences.append([f"{path}/{e1.tag}", "Tail", e1.tail, e2.tail])
    if e1.attrib != e2.attrib:
        differences.append([f"{path}/{e1.tag}", "Attributes", e1.attrib, e2.attrib])
    if len(e1) != len(e2):
        differences.append([f"{path}/{e1.tag}", "Children count", len(e1), len(e2)])
    for c1, c2 in zip(e1, e2):
        differences.extend(compareElements(c1, c2, path + "/" + e1.tag))
    return differences

def compareXml(file1, file2):
    sortedFile1 = file1.replace('.xml', '_sorted.xml')
    sortedFile2 = file2.replace('.xml', '_sorted.xml')
    tree1 = ET.parse(file1)
    tree2 = ET.parse(file2)
    root1 = tree1.getroot()
    root2 = tree2.getroot()
    sortElements(root1)
    sortElements(root2)
    tree1.write(sortedFile1)
    tree2.write(sortedFile2)
    tree1 = ET.parse(sortedFile1)
    tree2 = ET.parse(sortedFile2)
    root1 = tree1.getroot()
    root2 = tree2.getroot()
    return compareElements(root1, root2)

def generateCSVReport(differences, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Path", "Difference Type", "Base", "Actual"])
        for diff in differences:
            writer.writerow(diff)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--baseFile', help='XML base File Location')
    parser.add_argument('-a', '--actualFile', help='XML actual File Location')
    parser.add_argument('-o', '--outFile', help='CSV output File Location')
    args = parser.parse_args()
    base = args.baseFile
    actual = args.actualFile
    output = args.outFile
    differences = compareXml(base, actual)
    if len(differences) == 0:
        print("No differences found")
    else:
        print("Differences found")
        generateCSVReport(differences, output)
