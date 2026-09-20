"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 388 / 400
================================================================================
- Group: ThirdTest_pincode_group_39_parts_381_to_390
- Assigned PIN Codes: 48 (Range: 831004 to 833214)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_388.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_388.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_388"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-388] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "831004",
  "831005",
  "831006",
  "831007",
  "831009",
  "831011",
  "831012",
  "831013",
  "831014",
  "831015",
  "831016",
  "831017",
  "831019",
  "831020",
  "831021",
  "832101",
  "832102",
  "832103",
  "832104",
  "832105",
  "832106",
  "832107",
  "832108",
  "832109",
  "832111",
  "832112",
  "832113",
  "832301",
  "832302",
  "832303",
  "832304",
  "832401",
  "832402",
  "832403",
  "832404",
  "833101",
  "833102",
  "833103",
  "833104",
  "833105",
  "833106",
  "833201",
  "833202",
  "833203",
  "833204",
  "833212",
  "833213",
  "833214"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "831004": {
    "pincode": "831004",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chhota Govindpur BO",
      "Ghorabandha BO",
      "Luabasa BO",
      "Telco Works SO",
      "Indranagar SO",
      "Kalimati Market SO",
      "N/A",
      "Telco Plaza Market SO",
      "Hurlung BO"
    ]
  },
  "831005": {
    "pincode": "831005",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Kadma SO"
    ]
  },
  "831006": {
    "pincode": "831006",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Jugsalai SO"
    ]
  },
  "831007": {
    "pincode": "831007",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Burma Mines SO",
      "N M L SO"
    ]
  },
  "831009": {
    "pincode": "831009",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Agrico SO"
    ]
  },
  "831011": {
    "pincode": "831011",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Sonari SO East Singhbhum"
    ]
  },
  "831012": {
    "pincode": "831012",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Bhilaipahari BO",
      "Hirachuni BO",
      "Kapali BO",
      "Pipla BO",
      "Belajuri BO",
      "Mango SO",
      "Azadnagar SO",
      "MGM Medical College SO"
    ]
  },
  "831013": {
    "pincode": "831013",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Adityapur SO"
    ]
  },
  "831014": {
    "pincode": "831014",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "NIT SO"
    ]
  },
  "831015": {
    "pincode": "831015",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Gobindpur Housing Colony SO"
    ]
  },
  "831016": {
    "pincode": "831016",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Rahargora SO"
    ]
  },
  "831017": {
    "pincode": "831017",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Baridih Colony SO"
    ]
  },
  "831019": {
    "pincode": "831019",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Birsanagar SO"
    ]
  },
  "831020": {
    "pincode": "831020",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Pardih"
    ]
  },
  "831021": {
    "pincode": "831021",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "N/A"
    ]
  },
  "832101": {
    "pincode": "832101",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Baharagora SO",
      "Sakra BO",
      "Angarpara BO",
      "Arong BO",
      "Bamdole BO",
      "Baragaria BO",
      "Benda BO",
      "Chandrapur BO",
      "Chhota Porulia BO",
      "Chitreshwar BO",
      "Damjuri BO",
      "Dandudih BO",
      "Darkhuli BO",
      "Gamharia BO",
      "Ghaspada BO",
      "Guhiapal BO",
      "Jamshola BO",
      "Jarapal BO",
      "Joypura BO",
      "Kaima BO",
      "Kaimi BO",
      "Kesharda BO",
      "Khandamouda BO",
      "Kumardubi BO",
      "Malua BO",
      "Matihana BO",
      "Mouda BO",
      "Murakati BO",
      "Ram Chandra Pur BO",
      "Bankata BO",
      "BANMAKRI BO"
    ]
  },
  "832102": {
    "pincode": "832102",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Amlatola BO",
      "Asanboni BO",
      "Bengo BO",
      "Bhatin BO",
      "Butgora BO",
      "Dhirol BO",
      "Dorkasai BO",
      "Kalikapur BO",
      "Manpur BO",
      "Rajdoha BO",
      "Hathibinda BO",
      "Jadugoda Mines SO",
      "Dhobani  BO",
      "Kuldiha BO"
    ]
  },
  "832103": {
    "pincode": "832103",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Moubhandar SO"
    ]
  },
  "832104": {
    "pincode": "832104",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Meria BO",
      "Khairbani BO",
      "Kuilisuta BO",
      "Badia BO",
      "Bankisole BO",
      "Bara Asti BO",
      "Barabotla BO",
      "Bhagabandhi BO",
      "Dumuria BO",
      "Kumarshol BO",
      "Kantasole BO",
      "Mosabani Mines SO",
      "Kantashol BO",
      "Palasbani BO",
      "Parulia BO",
      "Kharida BO",
      "Bara Kanjia BO"
    ]
  },
  "832105": {
    "pincode": "832105",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Laylan BO",
      "Bangurda BO",
      "Bankuchia BO",
      "Bidra BO",
      "Chirudih BO",
      "Gobarghusi BO",
      "Jorsiya BO",
      "Kamalpur BO",
      "Kasmar BO",
      "Kumir BO",
      "Lachhipur BO",
      "Pagda BO",
      "Rasik Nagar BO",
      "Dighi BO",
      "Patamda SO",
      "Sisda BO",
      "Mahulbani BO"
    ]
  },
  "832106": {
    "pincode": "832106",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Rakha Mines BO",
      "Matigora BO",
      "Rakha Copper Project SO"
    ]
  },
  "832107": {
    "pincode": "832107",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Gitilata BO",
      "Kendamundi BO",
      "Shankarda BO",
      "Tumung BO",
      "Sundarnagar SO"
    ]
  },
  "832108": {
    "pincode": "832108",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Gamharia SO",
      "Baramari BO",
      "Burudih BO",
      "Dugdha BO",
      "Nowagarh BO"
    ]
  },
  "832109": {
    "pincode": "832109",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Adityapur Industrial Area SO"
    ]
  },
  "832111": {
    "pincode": "832111",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Narwa Mines  SO"
    ]
  },
  "832112": {
    "pincode": "832112",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chhota Bangurda BO",
      "Laojora BO",
      "Kuiani BO",
      "Madhavpur BO",
      "Barachirka BO",
      "Beldih BO",
      "Koira BO",
      "Pokharia BO",
      "Paharpur BO",
      "Boram SO"
    ]
  },
  "832113": {
    "pincode": "832113",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Hata Chowk SO"
    ]
  },
  "832301": {
    "pincode": "832301",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chakulia SO",
      "Jamuwa BO",
      "Amlagora BO",
      "Balibandh BO",
      "Banasoli BO",
      "Baramara BO",
      "Bardikanpur BO",
      "Bend BO",
      "Bhalukbinda BO",
      "Bhandaru BO",
      "Gandanata BO",
      "Kalapathar BO",
      "Kalidaspur BO",
      "Katushole BO",
      "Kendadangri BO",
      "Lodhasholi BO",
      "Manusmuria BO",
      "Mural BO",
      "Rupuskundi BO",
      "Sardiha BO",
      "Simdi BO",
      "Chingra BO",
      "Pathara BO",
      "Sonahatu BO",
      "Purnapani BO",
      "Jugitupa BO",
      "Bhaduakocha BO",
      "Ulda BO",
      "Hariniya  BO"
    ]
  },
  "832302": {
    "pincode": "832302",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Dhalbhumgarh SO",
      "Malkudi BO",
      "Arjunbera BO",
      "Baliaguri BO",
      "Balijuri BO",
      "Bhalki BO",
      "Gohaldangra BO",
      "Gurabanda BO",
      "Kaliam BO",
      "Kantaboni BO",
      "Kokpara BO",
      "Matiabandhi BO",
      "Mohulishole BO",
      "Murathakura BO",
      "Nutangarh BO",
      "Panduda BO",
      "Pitajuri BO",
      "Rautara BO",
      "Shyam Sunderpur BO",
      "Singhpura BO",
      "Kanas BO",
      "Kalaiya BO",
      "Forest Block BO"
    ]
  },
  "832303": {
    "pincode": "832303",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Ghatsila SO",
      "Asna BO",
      "Bankati BO",
      "Barajuri BO",
      "Dobha BO",
      "Edelbera BO",
      "Haldajuri BO",
      "Jhantijharna BO",
      "Karaduba BO",
      "Kendadih BO",
      "Mohanpur BO",
      "Pungora BO",
      "Surda Mines BO",
      "Gopalpur BO",
      "Benashol BO",
      "Chukripara BO"
    ]
  },
  "832304": {
    "pincode": "832304",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Barakurshi BO",
      "Beko BO",
      "Chorinda BO",
      "Hendaljuri BO",
      "Kesharpur BO",
      "Suklara BO",
      "Mahulia SO",
      "Ulda BO"
    ]
  },
  "832401": {
    "pincode": "832401",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chandil SO",
      "Adardih BO",
      "Bamni BO",
      "Bamni Chaliyama BO",
      "Bareda BO",
      "Bhadudih BO",
      "Chainpur BO",
      "Chelgu BO",
      "Ghoraling BO",
      "Jugilong BO",
      "Kanderbera BO",
      "Madhupur BO",
      "Nimdih BO",
      "Simagunda BO",
      "Suksari BO",
      "Tankocha BO",
      "Tilla BO"
    ]
  },
  "832402": {
    "pincode": "832402",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Hudu BO",
      "Kandra SO Seraikelakharsawan"
    ]
  },
  "832403": {
    "pincode": "832403",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Choura BO",
      "Dulmi BO",
      "Ichadih BO",
      "Janum BO",
      "Kukru BO",
      "Muru BO",
      "Shirum BO",
      "Situ BO",
      "Soro BO",
      "Tiruldih SO"
    ]
  },
  "832404": {
    "pincode": "832404",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chowka SO",
      "Bandu BO",
      "Bansa BO",
      "Buruhatu BO",
      "Chipri BO",
      "Deoltand BO",
      "Gaurangkocha BO",
      "Ghatdulmi BO",
      "Hesakocha BO",
      "Ichagarh BO",
      "Jhabri BO",
      "Kashidih BO",
      "Khunti BO",
      "Lepatand BO",
      "Moisara BO",
      "Nadisai BO",
      "Sidhdih BO",
      "Tamari BO",
      "Tikar BO",
      "Tuta BO",
      "Urmal BO"
    ]
  },
  "833101": {
    "pincode": "833101",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Galudih BO",
      "Amda SO",
      "Gopidih BO",
      "Padampur BO"
    ]
  },
  "833102": {
    "pincode": "833102",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chakradharpur SO",
      "Hurangda BO",
      "Baipee BO",
      "Barabamboo BO",
      "Bhalupani BO",
      "Chainpur Khas BO",
      "Chirubera BO",
      "Gopinathpur BO",
      "Hathia BO",
      "Hesalkuti BO",
      "Jamid BO",
      "Jharjharia BO",
      "Jojokurma BO",
      "Kera BO",
      "Keraikela BO",
      "Lotapahar BO",
      "Mahulpani BO",
      "Nakti BO",
      "Otar BO",
      "Roladih BO",
      "Sarjamhatu BO",
      "Toklo BO",
      "Unchibita BO",
      "Landupada BO",
      "Chakradharpur Bazar SO",
      "Chakradharpur Colony SO",
      "Nalita BO",
      "Kulitorang BO",
      "Hoyahatu BO",
      "Kendu BO",
      "Itore BO",
      "Chandri BO",
      "Itihasa BO",
      "Gulkera BO",
      "Surguru BO",
      "Hatnatorang BO",
      "Bharania BO"
    ]
  },
  "833103": {
    "pincode": "833103",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Goilkera SO",
      "Bari BO",
      "Kairam BO",
      "Tuniagajpur BO",
      "Bila BO",
      "Bara BO",
      "Kebra BO",
      "Arahasa BO",
      "Tarkatkocha BO",
      "Sarugara BO"
    ]
  },
  "833104": {
    "pincode": "833104",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Manoharpur SO West Singhbhum",
      "Makranda BO",
      "Lailor BO",
      "Baikera BO",
      "Barposh BO",
      "Dumrita BO",
      "Patherbasa BO",
      "Posiata BO",
      "Rajanand Pur BO",
      "Robkera BO",
      "Urkia BO",
      "Barakenduda BO",
      "Digha BO",
      "Kaida BO",
      "Nandpur BO",
      "Dimduli BO",
      "Kolpotka BO",
      "Harta BO",
      "Tomdel BO",
      "Jharbera BO",
      "Binju BO",
      "Rundhikocha BO"
    ]
  },
  "833105": {
    "pincode": "833105",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Sonua SO",
      "Golmunda BO",
      "Porahat BO",
      "Asantalia BO",
      "Bhalurungi BO",
      "Jarakel BO",
      "Kuira BO",
      "Lonjo BO",
      "Balijori BO",
      "Kadamdiha  BO",
      "Dewanbir BO"
    ]
  },
  "833106": {
    "pincode": "833106",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chiria SO",
      "Chotanagra BO",
      "Salai BO",
      "Gangda BO"
    ]
  },
  "833201": {
    "pincode": "833201",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Chaibasa HO",
      "Bari Bazar SO"
    ]
  },
  "833202": {
    "pincode": "833202",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Tata College Chaibasa SO",
      "Asura BO",
      "Singhpokharia BO",
      "Kelende  BO",
      "Tuibir  BO"
    ]
  },
  "833203": {
    "pincode": "833203",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Jagannathpur SO",
      "Kasira BO",
      "Kochra BO",
      "Maluka BO",
      "Todanghatu BO",
      "Pokharpi BO",
      "Barananda BO"
    ]
  },
  "833204": {
    "pincode": "833204",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Tekrahatu BO",
      "DOPAI B.O.",
      "JMP Chowk SO",
      "Bagabilla BO",
      "Barachiru BO",
      "Baralagia BO",
      "Baralagra BO",
      "Barkella BO",
      "Barkundia BO",
      "Bharbharia BO",
      "Bhoya BO",
      "Chittimitti BO",
      "Dholadih BO",
      "Ghagri BO",
      "Gitilipi BO",
      "Guira BO",
      "Gulia BO",
      "Icha BO",
      "Jhalak BO",
      "Kathbari BO",
      "Keshargaria BO",
      "Khedchalan BO",
      "Kheriatangar BO",
      "Khuntpani BO",
      "Kokcho BO",
      "Nakahasa BO",
      "Narsanda BO",
      "Pandrasali BO",
      "Pilka BO",
      "Purunia BO",
      "Roro BO",
      "Sarda BO",
      "Tantnagar BO",
      "Tonto BO",
      "KHASHPOKHERIYA   B.O.",
      "Tentra BO",
      "RUIDIH B.O.",
      "Baraguira BO",
      "Pandabir BO",
      "Baduri BO",
      "Harila BO",
      "Bara Torlo BO",
      "Angardih BO",
      "Padsa BO",
      "Kursi BO",
      "Khaspokhria BO",
      "Ulirajbasa BO",
      "Ruidih BO",
      "Ipilsingi BO",
      "Karlajodi BO",
      "Tentera BO",
      "Simbiya BO"
    ]
  },
  "833212": {
    "pincode": "833212",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Danguaposi SO"
    ]
  },
  "833213": {
    "pincode": "833213",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "Gua SO"
    ]
  },
  "833214": {
    "pincode": "833214",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Singhbhum Division",
    "offices": [
      "CHOTA MAHULDIYA  B.O.",
      "ANGARPADA   B.O.",
      "CHOTA RAIKAMAN   B.O.",
      "SONAPOSI  B.O.",
      "Siyaljora BO",
      "TBDumuria BO",
      "Gumuria BO",
      "Hatgamaria SO",
      "Andhari BO",
      "Balandia BO",
      "Balibandh BO",
      "Barajamni BO",
      "Benisagar BO",
      "Bensakaranjia BO",
      "Dhobadhobin BO",
      "Jaintgarh BO",
      "Jamdih BO",
      "Karanjia BO",
      "Khairbandh BO",
      "Khairpal BO",
      "Khurpose BO",
      "Kumardungi BO",
      "Kundiadhar BO",
      "Majhgaon BO",
      "NAshram BO",
      "Parsagar Keshna BO",
      "Punga BO",
      "Ruia BO",
      "Sindri Guiya BO",
      "Dikubalkand  BO",
      "Patajaint BO",
      "Angarpada BO",
      "Kumirta BO",
      "Chhota Mahuldiha BO",
      "Bhangaon BO",
      "Sonaposi BO",
      "Chhota Raikaman BO",
      "Asanpat BO",
      "Bera Mundui BO",
      "Baliaposi  BO",
      "Kusmunda BO",
      "Bhonda BO",
      "Parsa BO",
      "Barusai BO",
      "Sindriguia BO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
