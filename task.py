import sys, getopt, json, os
import mysql.connector
import datetime
from mysql.connector.constants import ClientFlag
config = {
    'user': 'root',
    'password': 'password123',
    'host': '34.122.64.190',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': 'server-ca.pem',
    'ssl_cert': 'client-cert.pem',
    'ssl_key': 'client-key.pem',
    'database' : 'jobdata'
}
cnxn = mysql.connector.connect(**config)
cursor = cnxn.cursor()  # initialize connection cursor
def main(argv):
   	inputfile = ''
   	outputfile = ''
   	try:
   	  	opts, inputfiles = getopt.getopt(argv,["ifile=","ofile="])
   	except getopt.GetoptError:
   	  	sys.exit(2)
   	new_tags = []
   	new_jobs = []
   	new_customers = []
   	if len(inputfiles) == 2:
   		if inputfiles[0][-5:] == ".json" and inputfiles[1][-5:] == ".json":
   			if os.path.isfile(inputfiles[0]) and os.path.isfile(inputfiles[1]):
	   			with open(inputfiles[0]) as f:
	   				tag_data = json.load(f)
	   			with open(inputfiles[1]) as f:
	   				job_data = json.load(f)
	   			with open(inputfiles[1]) as f:
	   				customer = json.load(f)
	   			with open("sample_tag.json") as f:
	   				sample_tag = json.load(f)
	   			with open("sample_job.json") as f:
	   				sample_job = json.load(f)
	   			with open("sample_customer.json") as f:
	   				sample_customer = json.load(f)
	   			if tag_data and job_data and customer:
				   	tuple_tag_data = ()
				   	list_tuple_tag_data = list(tuple_tag_data)
				   	list_tag_data = []
				   	check_repeat_id = []
				   	num = False
				   	check = False
				   	if tag_data["response"]:
				   		for x in tag_data["response"]:
				   			for i in range(len(check_repeat_id)):
				   				if x["id"] == check_repeat_id[i]:
					   				num = True
					   		if num == True:
					   			num = False
					   			continue
					   		if sample_tag.keys() == x.keys():
					   			cursor.execute("SELECT id FROM tags")
					   			first_tag_data = cursor.fetchall()
					   			if first_tag_data:
					   				for m in first_tag_data:
					   					if m[0] == x["id"]:
					   						check = True
					   						break
					   				if check == False:
					   					new_tags.append(x)
					   				check = False

					   			else:
					   				pass
					   			list_tag_data.append(x["id"])
						   		check_repeat_id.append(x["id"])
						   		list_tag_data.append(x["company_id"])
						   		list_tag_data.append(x["mongo_id"])
						   		list_tag_data.append(x["cuid"])
						   		list_tag_data.append(x["title"])
						   		list_tag_data.append(x["type"])
						   		list_tag_data.append(x["color"])
						   		list_tag_data.append(x["created_at"])
						   		list_tag_data.append(x["updated_at"])
						   		list_tag_data.append(x["deleted_at"])
						   		list_tuple_tag_data.append(list_tag_data)
						   		list_tag_data = []
					   		else:
					   			print("Not a relevant fields in json data of this file!") # exception when no some field
					   	tuple_tag_data = tuple(list_tuple_tag_data)
					   	
				   	else:
				   		print("No relevant data in this file!") # exception when no response
				   	tuple_job_data = ()
				   	list_tuple_job_data = list(tuple_job_data)
				   	list_job_data = []
				   	job_tag		  = ()
				   	list_job_tag  = list(job_tag)
				   	list_tag 	  = []
				   	check_repeat_id = []
				   	num = False
				   	check = False
				   	if job_data["response"]:
				   		for x in job_data["response"]:
				   			for i in range(len(check_repeat_id)):
				   				if x["id"] == check_repeat_id[i]:
					   				num = True
					   		if num == True:
					   			num = False
					   			continue
					   		if sample_job.keys() == x.keys():
					   			cursor.execute("SELECT id FROM jobs")
					   			first_job_data = cursor.fetchall()
					   			if first_job_data:
					   				for m in first_job_data:
					   					if m[0] == x["id"]:
					   						check = True
					   						break
					   				if check == False:
					   					new_jobs.append(x)
					   				check = False

					   			else:
					   				pass
					   			if x["tags"]:
					   				list_tag.append(x["id"])
					   				list_tag.append(x["tags"])
					   				list_job_tag.append(list_tag)
					   				list_tag = []
					   			list_job_data.append(x["id"])
						   		check_repeat_id.append(x["id"])
						   		list_job_data.append(x["company_id"])
						   		list_job_data.append(x["mongo_id"])
						   		list_job_data.append(x["cuid"])
						   		list_job_data.append(x["author_id"])
						   		list_job_data.append(x["customer_id"])
						   		list_job_data.append(x["job_type"])
						   		list_job_data.append(x["status"])
						   		list_job_data.append(x["start_time"])
						   		list_job_data.append(x["end_time"])
						   		list_job_data.append(x["due_date"])
						   		list_job_data.append(x["billing"])
						   		list_job_data.append(x["assignment_count"])
						   		list_job_data.append(x["in_progress_status_log"])
						   		list_job_data.append(x["invoice_status"])
						   		list_job_data.append(x["notes"])
						   		list_job_data.append(x["location"])
						   		if (x["location_coords"]):
						   			list_job_data.append(str(x["location_coords"]["latitude"]) + "," + str(x["location_coords"]["longitude"]))
						   		else:
						   			list_job_data.append(x["location_coords"])
						   		list_job_data.append(x["tags_string"])
						   		list_job_data.append(x["created_at"])
						   		list_job_data.append(x["updated_at"])
						   		list_job_data.append(x["deleted_at"])
						   		list_job_data.append(x["project_id"])
						   		list_job_data.append(x["recurring_parent_id"])
						   		list_job_data.append(x["upcoming_job_notified"])
						   		list_job_data.append(x["detached_from_recurring_parent"])
						   		list_job_data.append(x["assignments"])
						   		list_tuple_job_data.append(list_job_data)
						   		list_job_data = []
					   		else:
					   			print("Not a relevant fields in json data of this file!")
					   	job_tag = tuple(list_job_tag)
					   	#print(job_tag)
				   	else:
				   		print("No relevant data in this file!")
				   	tuple_job_data = tuple(list_tuple_job_data)
				   	tuple_customer = ()
				   	customer_tag   = ()
				   	list_customer_tag = list(customer_tag)
				   	list_customer_data  = []
				   	list_tuple_customer = list(tuple_customer)
				   	list_customer 	= []
				   	check_repeat_id = []
				   	num = False
				   	check = False
				   	command = ""
				   	if customer["response"]:
				   		for x in customer["response"]:
				   			for i in range(len(check_repeat_id)):
				   				if x["customer"]["id"] == check_repeat_id[i]:
					   				num = True
					   		if num == True:
					   			num = False
					   			continue

					   		if sample_customer.keys() == x["customer"].keys():
					   			cursor.execute("SELECT id FROM customers")
					   			first_customers_data = cursor.fetchall()
					   			if first_customers_data:
					   				for m in first_customers_data:
					   					if m[0] == x["customer"]["id"]:
					   						check = True
					   						break
					   				if check == False:
					   					new_customers.append(x["customer"])
					   				check = False

					   			else:
					   				pass
					   			if x["customer"]["tags"]:
					   				list_customer_data.append(x["customer"]["id"])
					   				list_customer_data.append(x["customer"]["tags"])
					   				list_customer_tag.append(list_customer_data)
					   				list_customer_data = []
					   			check_repeat_id.append(x["customer"]["id"])
						   		list_customer.append(x["customer"]["id"])
						   		list_customer.append(x["customer"]["company_id"])
						   		list_customer.append(x["customer"]["mongo_id"])
						   		list_customer.append(x["customer"]["email"])
						   		list_customer.append(x["customer"]["phone"])
						   		list_customer.append(x["customer"]["city"])
						   		list_customer.append(x["customer"]["state"])
						   		list_customer.append(x["customer"]["notes"])
						   		list_customer.append(x["customer"]["address_1"])
						   		list_customer.append(x["customer"]["address_2"])
						   		list_customer.append(x["customer"]["cuid"])
						   		list_customer.append(x["customer"]["first_name"])
						   		list_customer.append(x["customer"]["middle_name"])
						   		list_customer.append(x["customer"]["last_name"])
						   		list_customer.append(x["customer"]["company_name"])
						   		list_customer.append(x["customer"]["zip_code"])
						   		list_customer.append(x["customer"]["searchable"])
						   		list_customer.append(x["customer"]["sort_key"])
						   		list_customer.append(x["customer"]["status"])
						   		list_customer.append(x["customer"]["has_different_billing_address"])
						   		list_customer.append(x["customer"]["alt_email"])
						   		list_customer.append(x["customer"]["phone_e164"])
						   		list_customer.append(x["customer"]["qbo_id"])
						   		list_customer.append(x["customer"]["title"])
						   		list_customer.append(x["customer"]["suffix"])
						   		list_customer.append(x["customer"]["alt_phone"])
						   		list_customer.append(x["customer"]["billing_address_1"])
						   		list_customer.append(x["customer"]["billing_address_2"])
						   		list_customer.append(x["customer"]["billing_city"])
						   		list_customer.append(x["customer"]["billing_state"])
						   		list_customer.append(x["customer"]["fax"])
						   		list_customer.append(x["customer"]["skype"])
						   		list_customer.append(x["customer"]["assigned_to"])
						   		list_customer.append(x["customer"]["billing_zip_code"])
						   		list_customer.append(x["customer"]["secondary_first_name"])
						   		list_customer.append(x["customer"]["secondary_last_name"])
						   		list_customer.append(x["customer"]["secondary_email"])
						   		list_customer.append(x["customer"]["secondary_phone"])
						   		list_customer.append(x["customer"]["use_company_name"])
						   		list_customer.append(x["customer"]["dial_phone"])
						   		list_customer.append(x["customer"]["mobile_phone"])
						   		list_customer.append(x["customer"]["website"])
						   		list_customer.append(x["customer"]["qbo_sync_token"])
						   		list_customer.append(x["customer"]["last_qbo_sync"])
						   		list_customer.append(x["customer"]["created_at"])
						   		list_customer.append(x["customer"]["updated_at"])
						   		list_customer.append(x["customer"]["deleted_at"])
						   		list_customer.append(x["customer"]["parent_id"])
						   		list_customer.append(x["customer"]["postal_code"])
						   		list_customer.append(x["customer"]["alt_first_name"])
						   		list_customer.append(x["customer"]["alt_last_name"])
						   		list_customer.append(x["customer"]["lead_source"])
						   		list_customer.append(x["customer"]["service_address_1"])
						   		list_customer.append(x["customer"]["service_address_2"])
						   		list_customer.append(x["customer"]["service_address_city"])
						   		list_customer.append(x["customer"]["service_address_state"])
						   		list_customer.append(x["customer"]["service_address_zip_code"])
						   		list_customer.append(x["customer"]["job_notes"])
						   		list_customer.append(x["customer"]["account_type"])
						   		list_customer.append(x["customer"]["parent_id_previous"])
						   		list_customer.append(x["customer"]["address1"])
						   		list_customer.append(x["customer"]["xero_id"])
						   		list_customer.append(x["customer"]["is_sync"])
						   		list_customer.append(x["customer"]["notes_old"])
						   		list_customer.append(x["customer"]["different_billing_address"])
						   		list_customer.append(x["customer"]["xero_group_id"])
						   		list_customer.append(x["customer"]["dev_qbo"])
						   		list_customer.append(x["customer"]["source"])
						   		list_customer.append(x["customer"]["secondar_last_name"])
						   		list_customer.append(x["customer"]["sync_status"])
						   		list_customer.append(x["customer"]["xero_guid"])
						   		list_customer.append(x["customer"]["display_name"])
						   		list_customer.append(x["customer"]["customfields"])
						   		list_tuple_customer.append(list_customer)
						   		list_customer = []
					   		else:
					   			print("Not a relevant fields in json data of this file!")
					   	customer_tag = tuple(list_customer_tag)
					   	#print(customer_tag)
				   	else:
				   		print("No relevant data in this file!")
				   	tuple_customer = tuple(list_tuple_customer)
				   	cursor.execute("SELECT job_id, tag_id FROM job_tag")
				   	job_tag_result = cursor.fetchall()
				   	check_repeat_job_tag = False
				   	check_repeat_customer_tag = False
				   	# for i in range(len(job_tag_result)):
				   	# 	for j in range(len(job_tag)):
				   	# 		for m in range(len(job_tag[j][1])):
				   	# 			if job_tag_result[i][0] == job_tag[j][0] and job_tag_result[i][1] == job_tag[j][1][m]:
				   	# 				check_repeat_job_tag = True		
				   	sql = "INSERT INTO job_tag (job_id, tag_id) VALUES (%s, %s)"
				   	#if check_repeat_job_tag == False:
				   	if job_tag_result:
				   		query_del = ("DELETE FROM job_tag WHERE job_id = %s")
				   		for x in job_tag:
				   			cursor.execute(query_del, (x[0],))
				   			cnxn.commit()
				   		for x in job_tag:
					   		for y in x[1]:
					   			cursor.execute(sql, (x[0], y))
					   			cnxn.commit()
					   	print("Successfully updated job_tag")
				   	else:
				   		for x in job_tag:
					   		for y in x[1]:
					   			cursor.execute(sql, (x[0], y))
					   			cnxn.commit()
					   	print("Successfully uploaded job_tag")
				   	#print(tuple_customer)
				   	#print(tuple_job_data)
				   	cursor.execute("SELECT customer_id, tag_id FROM customer_tag")
				   	customer_tag_result = cursor.fetchall()
				   	sql = "INSERT INTO customer_tag (customer_id, tag_id) VALUES (%s, %s)"
				   	if customer_tag_result:
				   		query_del = ("DELETE FROM customer_tag WHERE customer_id = %s")
				   		for x in customer_tag:
				   			cursor.execute(query_del, (x[0],))
				   			cnxn.commit()
				   		for x in customer_tag:
					   		for y in x[1]:
					   			cursor.execute(sql, (x[0], y))
					   			cnxn.commit()
				   	else:
				   		for x in customer_tag:
					   		for y in x[1]:
					   			cursor.execute(sql, (x[0], y))
					   			cnxn.commit()
				   	for i in range(len(tuple_job_data)):
				   		for j in range(len(tuple_job_data[i])):
				   			if type(tuple_job_data[i][j]) == list:
				   				tuple_job_data[i][j] = str(tuple_job_data[i][j]).replace("[", "").replace("]", "")
				   	for i in range(len(tuple_customer)):
				   		for j in range(len(tuple_customer[i])):
				   			if type(tuple_customer[i][j]) == list:
				   				tuple_customer[i][j] = str(tuple_customer[i][j]).replace("[", "").replace("]", "")
				   	#print(tuple_job_data)
				   	#print(tuple_tag_data)
				   	cursor.execute("SELECT id FROM tags")
				   	tag_result = cursor.fetchall()
				   	check_repeat_database_tag = False
				   	check_repeat_database_job = False
				   	check_repeat_database_customer = False
				   	for i in range(len(tag_result)):
				   		for j in range(len(tuple_tag_data)):
				   			if tag_result[i][0] == tuple_tag_data[j][0]:
				   				check_repeat_database_tag = True
				   				update_tag(tuple_tag_data[j][0], tuple_tag_data[j], cnxn, cursor)
				   	cursor.execute("SELECT id FROM jobs")
				   	job_result = cursor.fetchall()
				   	for i in range(len(job_result)):
				   		for j in range(len(tuple_job_data)):
				   			if job_result[i][0] == tuple_job_data[j][0]:
				   				check_repeat_database_job = True
				   				update_job(tuple_job_data[j][0], tuple_job_data[j], cnxn, cursor)
				   	cursor.execute("SELECT id FROM customers")
				   	customer_result = cursor.fetchall()
				   	for i in range(len(customer_result)):
				   		for j in range(len(tuple_customer)):
				   			if customer_result[i][0] == tuple_customer[j][0]:
				   				check_repeat_database_customer = True
				   				update_customer(tuple_customer[j][0], tuple_customer[j], cnxn, cursor)
				   	if check_repeat_database_tag == False :
				   		query_tag = ("INSERT INTO tags (id, company_id, mongo_id, cuid, title, type, color, created_at, updated_at, deleted_at) "
					   					"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
					   	cursor.executemany(query_tag, tuple_tag_data)
					   	cnxn.commit()
					   	print("Successfully uploaded tag_data!")
					   	command = "create"
				   	else:
					   	print("Successfully updated tag_data!")
					   	command = "update"
					   	if new_tags:
					   		query_tag = ("INSERT INTO tags (id, company_id, mongo_id, cuid, title, type, color, created_at, updated_at, deleted_at) "
					   					"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
					   		print(new_tags)
					   		for x in new_tags:
						   		data = (x["id"], x["company_id"], x["mongo_id"], x["cuid"],x["title"], x["type"], x["color"], x["created_at"], x["updated_at"], x["deleted_at"])
						   		cursor.execute(query_tag, data)
						   		cnxn.commit()
				   	if check_repeat_database_job == False:
				   		query_job = ("INSERT INTO jobs (id, company_id, mongo_id, cuid, author_id, customer_id, job_type, status, start_time, end_time, due_date, billing, assignment_count, in_progress_status_log, invoice_status, notes, location, location_coords, tags_string, created_at, updated_at, deleted_at, project_id, recurring_parent_id, upcoming_job_notified, detached_from_recurring_parent, assignments) "
			   					"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
				   		cursor.executemany(query_job, tuple_job_data)
				   		cnxn.commit()
				   		print("Successfully uploaded job_data!")
				   		command = "create"
				   	else:
				   		print("Successfully updated job_data!")
				   		command = "update"
				   		if new_jobs:
					   		query_job = ("INSERT INTO jobs (id, company_id, mongo_id, cuid, author_id, customer_id, job_type, status, start_time, end_time, due_date, billing, assignment_count, in_progress_status_log, invoice_status, notes, location, location_coords, tags_string, created_at, updated_at, deleted_at, project_id, recurring_parent_id, upcoming_job_notified, detached_from_recurring_parent, assignments) "
			   					"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
					   		for x in new_jobs:
						   		data = (x["id"], x["company_id"], x["mongo_id"], x["cuid"],x["author_id"], x["customer_id"], x["job_type"], x["status"], x["start_time"], x["end_time"], x["due_date"], x["billing"], x["assignment_count"], x["in_progress_status_log"], x["invoice_status"], x["notes"], x["location"], str(x["location_coords"]).replace("{","").replace("}", ""), x["tags_string"], x["created_at"], x["updated_at"], x["deleted_at"], x["project_id"], x["recurring_parent_id"], x["upcoming_job_notified"], x["detached_from_recurring_parent"], str(x["assignments"]).replace("[", "").replace("]", ""))
						   		cursor.execute(query_job, data)
						   		cnxn.commit()
				   	if check_repeat_database_customer == False:
				   		query_customer = ("INSERT INTO customers (id, company_id, mongo_id, email, phone, city, state, notes, address_1, address_2, cuid, first_name, middle_name, last_name, company_name, zip_code, searchable, sort_key, status, has_different_billing_address, alt_email, phone_e164, qbo_id, title, suffix, alt_phone, billing_address_1, billing_address_2, billing_city, billing_state, fax, skype, assigned_to, billing_zip_code, secondary_first_name, secondary_last_name, secondary_email, secondary_phone, use_company_name, dial_phone, mobile_phone, website, qbo_sync_token, last_qbo_sync, created_at, updated_at, deleted_at, parent_id, postal_code, alt_first_name, alt_last_name, lead_source, service_address_1, service_address_2, service_address_city, service_address_state, service_address_zip_code, job_notes, account_type, parent_id_previous, address1, xero_id, is_sync, notes_old, different_billing_address, xero_group_id, dev_qbo, source, secondar_last_name, sync_status, xero_guid, display_name, customfields) "
				   		" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
					   	cursor.executemany(query_customer, tuple_customer)
					   	cnxn.commit()
					   	print("Successfully uploaded customer_data!")
					   	command = "create"
				   	else:
				   		print("Successfully updated customer_data!")
				   		command = "update"
				   		if new_customers:
				   			query_customer = ("INSERT INTO customers (id, company_id, mongo_id, email, phone, city, state, notes, address_1, address_2, cuid, first_name, middle_name, last_name, company_name, zip_code, searchable, sort_key, status, has_different_billing_address, alt_email, phone_e164, qbo_id, title, suffix, alt_phone, billing_address_1, billing_address_2, billing_city, billing_state, fax, skype, assigned_to, billing_zip_code, secondary_first_name, secondary_last_name, secondary_email, secondary_phone, use_company_name, dial_phone, mobile_phone, website, qbo_sync_token, last_qbo_sync, created_at, updated_at, deleted_at, parent_id, postal_code, alt_first_name, alt_last_name, lead_source, service_address_1, service_address_2, service_address_city, service_address_state, service_address_zip_code, job_notes, account_type, parent_id_previous, address1, xero_id, is_sync, notes_old, different_billing_address, xero_group_id, dev_qbo, source, secondar_last_name, sync_status, xero_guid, display_name, customfields) "
				   				" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
				   			for x in new_customers:
						   		data = (x["id"], x["company_id"], x["mongo_id"], x["email"],x["phone"], x["city"], x["state"], x["notes"], x["address_1"], x["address_2"], x["cuid"], x["first_name"], x["middle_name"], x["last_name"], x["company_name"], x["zip_code"], x["searchable"], x["sort_key"], x["status"], x["has_different_billing_address"], x["alt_email"], x["phone_e164"], x["qbo_id"], x["title"], x["suffix"], x["alt_phone"], x["billing_address_1"], x["billing_address_2"], x["billing_city"], x["billing_state"], x["fax"], x["skype"], x["assigned_to"], x["billing_zip_code"], x["secondary_first_name"], x["secondar_last_name"], x["secondary_email"], x["secondary_phone"], x["use_company_name"], x["dial_phone"], x["mobile_phone"], x["website"], x["qbo_sync_token"], x["last_qbo_sync"], x["created_at"], x["updated_at"], x["deleted_at"], x["parent_id"], x["postal_code"], x["alt_first_name"], x["alt_last_name"], x["lead_source"], x["service_address_1"], x["service_address_2"], x["service_address_city"], x["service_address_state"], x["service_address_zip_code"], x["job_notes"], x["account_type"], x["parent_id_previous"], x["address1"], x["xero_id"], x["is_sync"], x["notes_old"], x["different_billing_address"], x["xero_group_id"], x["dev_qbo"], x["source"], x["secondar_last_name"], x["sync_status"], x["xero_guid"], x["display_name"], str(x["customfields"]))
						   		cursor.execute(query_customer, data)
						   		cnxn.commit()

				   	print("\nCreated history!\n")
			   		query_history = ("INSERT INTO history (command, created_at, file_1, file_2) VALUES (%s, %s, %s, %s)")
				   	history_data = (command, datetime.datetime.now(), inputfiles[0], inputfiles[1])
				   	cursor.execute(query_history, history_data)
				   	cnxn.commit()
	   			else:
	   				print("Data is not exists!")
   			else:
   				print("No this named file!")  			
   		else:
   			print("File should have json extension!")
   	else:
   		print("You input only 2 files 'tag_data' and 'job_data.json' !")
   		
def update_job(id, data, cnxn, cursor):
	query_job = ("UPDATE jobs SET company_id = %s, mongo_id = %s, cuid= %s, author_id = %s, customer_id = %s, job_type = %s, status = %s, start_time = %s, end_time = %s, due_date = %s, billing = %s, assignment_count = %s, in_progress_status_log = %s, invoice_status = %s, notes = %s, location = %s, location_coords = %s, tags_string = %s, created_at = %s, updated_at = %s, deleted_at = %s, project_id = %s, recurring_parent_id = %s, upcoming_job_notified = %s, detached_from_recurring_parent = %s, assignments = %s WHERE id = %s")
	data.append(id)
	cursor.execute(query_job, data[1:])
	cnxn.commit()
	
def update_tag(id, data, cnxn, cursor):
	query_tag = ("UPDATE tags SET company_id = %s, mongo_id = %s, cuid = %s, title = %s, type = %s, color = %s, created_at = %s, updated_at = %s, deleted_at = %s WHERE id = %s")
	data.append(id)
	cursor.execute(query_tag, data[1:])
	cnxn.commit()
	
def update_customer(id, data, cnxn, cursor):
	query_customer = ("UPDATE customers SET company_id = %s, mongo_id = %s, email = %s, phone = %s, city = %s, state = %s, notes = %s, address_1 = %s, address_2 = %s, cuid = %s, first_name = %s, middle_name = %s, last_name = %s, company_name = %s, zip_code = %s, searchable = %s, sort_key = %s, status = %s, has_different_billing_address = %s, alt_email = %s, phone_e164 = %s, qbo_id = %s, title = %s, suffix = %s, alt_phone = %s, billing_address_1 = %s, billing_address_2 = %s, billing_city = %s, billing_state = %s, fax = %s, skype = %s, assigned_to = %s, billing_zip_code = %s, secondary_first_name = %s, secondary_last_name = %s, secondary_email = %s, secondary_phone = %s, use_company_name = %s, dial_phone = %s, mobile_phone = %s, website = %s, qbo_sync_token = %s, last_qbo_sync = %s, created_at = %s, updated_at = %s, deleted_at = %s, parent_id = %s, postal_code = %s, alt_first_name = %s, alt_last_name = %s, lead_source = %s, service_address_1 = %s, service_address_2 = %s, service_address_city = %s, service_address_state = %s, service_address_zip_code = %s, job_notes = %s, account_type = %s, parent_id_previous = %s, address1 = %s, xero_id = %s, is_sync = %s, notes_old = %s, different_billing_address = %s, xero_group_id = %s, dev_qbo = %s, source = %s, secondar_last_name = %s, sync_status = %s, xero_guid = %s, display_name = %s, customfields = %s WHERE id = %s")
	data.append(id)
	cursor.execute(query_customer, data[1:])
	cnxn.commit()
	
if __name__ == "__main__":
	main(sys.argv[1:])