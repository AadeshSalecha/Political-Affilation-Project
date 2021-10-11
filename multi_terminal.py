import time
import sys
import subprocess

###################################################################################
#                                                                                 #
#  Usage: python3 multi_terminal.py api_keys_file inputs_file to_run.py min_index # 
#                                                                                 #
###################################################################################

def main():
  # If not enough arguments
  if(len(sys.argv) != 5):
    print("Usage: python3 multi_terminal.py api_keys_file inputs_file to_run.py min_index")
    sys.exit(0)

  # Open API_keys
  api_keys_file = sys.argv[1]
  keys = read_api_keys(api_keys_file)

  # Open Inputs
  inputs_file = sys.argv[2]
  inputs = read_inputs(inputs_file)

  program_to_run = sys.argv[3]

  # Working keys 
  keys = [1, 3, 4, 15, 7, 13, 9, 11, 40, 48, 49, 50, 77]
  
  if(len(keys) < len(inputs)):
    print("Not enough keys for all inputs")

  min_index = int(sys.argv[4])

  # Open in Terminals
  for i in range(min_index, min_index+min(len(keys), len(inputs))):
    command = "gnome-terminal --tab -- " + construct_command(to_run = program_to_run, key_num = keys[i - min_index], data_file_num = inputs[i])
    print(command)
    subprocess.Popen(command, shell = True)
    # subprocess.call(['gnome-terminal', '-x', construct_command(to_run = program_to_run, key_num = keys[i], data_file_num = i)])
    time.sleep(1)

def construct_command(to_run, key_num, data_file_num):
  api_keys_file = sys.argv[1]
  return "python3 " + to_run + " " + api_keys_file + " " + str(data_file_num) + " " + str(key_num)

def read_api_keys(file_name):
  keys = []
  with open(file_name, 'r') as inptr:
    while True:
      comment = inptr.readline()
      consumer_key = inptr.readline().lstrip('consumer_key=').rstrip('\n')
      consumer_secret = inptr.readline().lstrip('consumer_secret=').rstrip('\n')
      access_key = inptr.readline().lstrip('access_key=').rstrip('\n')
      access_secret = inptr.readline().lstrip('access_secret=').rstrip('\n')
      blank_line = inptr.readline()

      if (not comment): ### EOF
        break

      keys.append([consumer_key, consumer_secret, access_key, access_secret])
  return keys

def read_inputs(file_name):
  inputs = []
  with open(file_name, 'r') as inptr:
    inputs = [x.rstrip('\n') for x in inptr.readlines()]
  return inputs

if __name__ == '__main__':
  main()