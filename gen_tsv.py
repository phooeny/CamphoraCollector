
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

import sys
import json


attr_list = ['ph','color_11', 'color_21', 'color_31', 'color_41', 'color_51', 'color_12', 'color_22', 'color_32', 'color_13', 'color_23', 'color_33', 'color_14', 'color_24', 'length_avg', 'length_32', 'length_31', 'length_30', 'length_29', 'length_28', 'length_27', 'length_26', 'length_25', 'micronaire_avg', 'micronaire_c1', 'micronaire_b1', 'micronaire_a', 'micronaire_b2', 'micronaire_c2', 'breaking_tenacity_avg', 'breaking_tenacity_max', 'breaking_tenacity_min', 'length_uniformty_avg', 'length_uniformty_max', 'length_uniformty_min', 'preparation_p1', 'preparation_p2','preparation_p3','package_num', 'weight_gross', 'weight_tare', 'weight_net', 'weight_conditoned', 'huichao_avg', 'hanza_avg', 'jiagongleixing' , 'ph_in_page' , 'jiagongqiye', 'cangku'];

if __name__ == "__main__":
	file_json = sys.argv[1];
	with open(file_json, 'r') as fd:
		for line in fd:
			line = line.strip();
			#print(line);
			dic_item = json.loads(line);
			val_list = [dic_item.get(k,'NaN') for k in attr_list];
			print('\t'.join(val_list));
				
