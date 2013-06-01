import urllib, urllib2, datetime, time, os, os.path
import re, csv, sys, ssl
import json, requests

# Get your api key at https://developers.google.com/civic-information/docs/using_api
api_key = "key"
polling_url = "https://www.googleapis.com/civicinfo/us_v1/voterinfo/4000/lookup?key=" + api_key
headers = {"Content-type": "application/json", "Accept": "text/plain"}

# Data sources.
input_file = "some-file"
input_dir = "./"
output_dir = "./"
file_extension = ".tsv"
field_separator = "\t"
line_terminator = "\n"

# Fields from the original file to include in the output file.
target_fields = ["dwid", "firstname", "middlename", "lastname", "namesuffix", "regaddrline1", "regaddrcity", "regaddrstate", "regaddrzip", "phone"]

# Field names to use for constructing the full address for Google.
address_field_name = "regaddrline1"
city_field_name = "regaddrcity"
state_field_name = "regaddrstate"
zip_field_name = "regaddrzip"

# Display current status every X lines.
display_status_interval = 25

# Misc settings.
output_file_name_suffix = "-geocoded"
max_consecutive_fails = 5
sleep_time = 0.001
hide_output_stream = "/dev/null"

input = open(input_dir + input_file + file_extension, 'r')
print "Opened", input_file + file_extension, "for input."

output_file = output_dir + input_file + output_file_name_suffix + file_extension

new_file = os.path.isfile(output_file) == 0

if new_file == 0:
  # We have tried to geocode this file before so pickup where we left off.
  old_file = open(output_file, "r")
  existing_lines = 0
  for line in old_file:
    existing_lines += 1
  old_file.close()
  print "Found", existing_lines, "existing lines."
    
output = open(output_file, 'a')
print "Opened", output_file, "for output."

header = input.readline().strip()
new_geocode_fields = ["status", "clean_address", "polling_location", "polling_line1", "polling_line2", "polling_line3", "polling_city", "polling_state", "polling_zip", "polling_notes", "polling_hours"]
output_fields = target_fields[:]
output_fields.extend(new_geocode_fields);

header_line = field_separator.join(output_fields) + line_terminator
if new_file:
  output.write(header_line)
header_fields = header.split(field_separator)
target_indices = []
code_counts = {}
index_list = {}

for key in target_fields:
  if key in header_fields:
    target_indices.append(header_fields.index(key)) 
    index_list[key] = header_fields.index(key)
  else:
    print "Error: could not locate key \"" + key + "\" in header fields."
input_count = 0
output_count = 0
row = 0

hide_output = open(hide_output_stream, "w")
show_output = sys.stdout

good = 0
bad = 0
_debug = 0

address_fix = re.compile("( (LOT|PMB|#APT|APT|#|RM|STE|TRLR|UNIT|BOX) +.+?|LOWR|UPPR|BSMT)$", re.IGNORECASE)

consecutive_fails = 0
for line in input:
  input_count += 1
  if new_file == 0 and input_count < existing_lines:
    # Skip this line until we get to a new line.
    continue
  fields = [f.replace("\r", "").replace("\n", "") for f in line.split(field_separator)]
  data_fields = [fields[index] for index in target_indices]

  address1 = data_fields[target_fields.index(address_field_name)]
  city = data_fields[target_fields.index(city_field_name)]
  state = data_fields[target_fields.index(state_field_name)]
  zip = data_fields[target_fields.index(zip_field_name)]

  # Remove apartment numbers and related trailing units so that polling place can be found.
  address1 = address_fix.sub("", address1)

  raw_address = address1 + ", " + city + ", " + state + " " + zip
  params = { "address": raw_address }
  results = {}

  for f in new_geocode_fields:
    results[f] = ""

  try:
    # If we're not debugging, submit the URL request.
    if _debug == 0:
      r = requests.post(polling_url, data=json.dumps(params), headers=headers)
      p =  r.json
      if p != None:
        results["status"] = p.get("status", "failed")
        if results["status"] == "success":
          if "normalizedInput" in p:
            n = p["normalizedInput"]
            results["clean_address"] = ", ".join([n["line1"], n["city"], " ".join([n["state"], n["zip"]])])

          if "pollingLocations" in p:
            loc = p["pollingLocations"][0]
            results["polling_location"] = loc["address"].get("locationName", "")
            results["polling_line1"] = loc["address"]["line1"]
            results["polling_line2"] = loc["address"].get("line2", "")
            results["polling_line3"] = loc["address"].get("line3", "")
            results["polling_city"] = loc["address"]["city"]
            results["polling_state"] = loc["address"]["state"]
            results["polling_zip"] = loc["address"].get("zip", "")
            results["polling_notes"] = " ".join(loc["notes"].splitlines())
            # results["polling_name"] = loc["name"]
            results["polling_hours"] = loc["pollingHours"]
          else:
            print "Could not find polling location for:", data_fields[0], "(" + state + "). Line:", input_count 


  except urllib2.HTTPError as e:
    # Didn't work for some reason.
    print "Bad request:", data_fields[0], "Error code:", e.code, "Reason:", e.reason
    bad += 1
  else:
    good += 1

  # Wait a bit so we don't overload the Google Maps API.
  time.sleep(sleep_time)

  if consecutive_fails > max_consecutive_fails:
    print "FAIL:", consecutive_fails, "fails in a row; stopping on line", str(input_count) + "."
    input.close()
    output.close()
    sys.exit()

  try: 
    extra_data = [results["status"], results["clean_address"], results["polling_location"], results["polling_line1"], results["polling_line2"], results["polling_line3"], results["polling_city"], results["polling_state"], results["polling_zip"], results["polling_notes"], results["polling_hours"]]
  except:
    print "error"
    
  data_fields.extend(extra_data)
  try: 
    output_line = field_separator.join(data_fields) + line_terminator
    output.write(output_line)
    output_count += 1
  except UnicodeDecodeError:
    # TODO: need to fix this quick unicode hack.
    print "Weird Unicode error! Record:", str(input_count), ", Address:", raw_address + "."
  except UnicodeEncodeError:
    # TODO: need to fix this quick unicode hack.
    print "Weird Unicode error! Record:", str(input_count) + ", Address:", raw_address + "."

  if input_count % display_status_interval == 0:
    print "Count:", str(input_count) + ", Good:", str(good) + ", Bad:", str(bad) + "."

  row += 1

print "Final input:", str(input_count) + ", Final output:", str(output_count) + "."
print "Good:", str(good) + ", Bad:", str(bad) + "."
input.close()
output.close()
