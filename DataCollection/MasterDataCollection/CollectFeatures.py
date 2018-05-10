import sys
with open(sys.path[0]+"/Feature_Data/business_wc_sorted", "r") as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content]
for line in content:
    print ("line ",line)
