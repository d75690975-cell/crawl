"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 381 / 400
================================================================================
- Group: ThirdTest_pincode_group_39_parts_381_to_390
- Assigned PIN Codes: 48 (Range: 814114 to 816101)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_381.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_381.csv & .json
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

PART_ID = "part_381"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-381] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "814114",
  "814115",
  "814116",
  "814118",
  "814119",
  "814120",
  "814131",
  "814133",
  "814141",
  "814142",
  "814143",
  "814144",
  "814145",
  "814146",
  "814147",
  "814148",
  "814149",
  "814150",
  "814151",
  "814152",
  "814153",
  "814154",
  "814155",
  "814156",
  "814157",
  "814158",
  "814160",
  "814165",
  "814166",
  "814167",
  "815301",
  "815302",
  "815311",
  "815312",
  "815313",
  "815314",
  "815315",
  "815316",
  "815317",
  "815318",
  "815351",
  "815352",
  "815353",
  "815354",
  "815355",
  "815357",
  "815359",
  "816101"
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
  "814114": {
    "pincode": "814114",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Deosang BT SO",
      "Gurukul Vidyapith BO",
      "Maladih BO",
      "Mathurapur BO",
      "Sakrigali BO",
      "Sangrampur Lorhiya BO",
      "Simra BO"
    ]
  },
  "814115": {
    "pincode": "814115",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Hindipith SO"
    ]
  },
  "814116": {
    "pincode": "814116",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Satsang SO"
    ]
  },
  "814118": {
    "pincode": "814118",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Basukinathdham SO",
      "Bagjhopa BO",
      "Baratanr BO",
      "Chamrabahiar BO",
      "Dhamnilata BO",
      "Haripur BO",
      "Laximipur BO",
      "Siktia BO",
      "Singhni BO"
    ]
  },
  "814119": {
    "pincode": "814119",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Kurwa SO",
      "Asansol BO",
      "Gamra BO",
      "Gando BO",
      "Gandrakhpur BO",
      "Ghasipur BO",
      "Gugisimal BO",
      "K Baskichowk BO",
      "Kathalia BO",
      "Kumrabad BO",
      "Makrampur BO",
      "Morbhanga BO",
      "Pahrudih BO",
      "Pattabari BO"
    ]
  },
  "814120": {
    "pincode": "814120",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Ghormara SO",
      "Bargo BO",
      "Dumariahat BO",
      "Gadibahirkunda BO",
      "Hariharpur BO",
      "Jhallar BO",
      "Kaladumaria BO",
      "Karma BO",
      "Morney BO",
      "Rajan Amar Kunda BO",
      "Sahara BO",
      "Satpahari BO",
      "Simarjore BO",
      "T Basdiha BO",
      "Bahjrikunda BO",
      "Jardaha BO"
    ]
  },
  "814131": {
    "pincode": "814131",
    "circle": "Bihar circle",
    "region": "East Region, Bhagalpur",
    "division": "Bhagalpur Division",
    "offices": [
      "Bank BO",
      "Bhorabazar  BO",
      "Bhorsar BO",
      "Inarabaran BO",
      "Jokta Fatehpur BO",
      "Kumartar BO",
      "Lohari BO",
      "Pelwa B.O",
      "Chandan SO"
    ]
  },
  "814133": {
    "pincode": "814133",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Godda SO",
      "Amlo BO",
      "Baksara BO",
      "Basantpur BO",
      "Chapri BO",
      "Deobandha BO",
      "Dubrajpur BO",
      "Fasia BO",
      "Gandhigram BO",
      "Gangtaklan BO",
      "Ghatdumariya BO",
      "Ghatkusmani BO",
      "Godda College BO",
      "Gumma BO",
      "Haripur Gorbana BO",
      "Hilaway BO",
      "Jaminipaharpur BO",
      "Khatnai BO",
      "Korkaghat BO",
      "Kouribahiar BO",
      "Latauna BO",
      "Lohbandha BO",
      "Lukluki BO",
      "Maheshpur BO",
      "Makhni BO",
      "Malini BO",
      "Malrampur BO",
      "Mohanpur BO",
      "Motia BO",
      "Motiya Dumariya BO",
      "Nepura BO",
      "Pathra BO",
      "Ramla Dikwani BO",
      "Saidapur BO",
      "Sarba BO",
      "Simarda BO"
    ]
  },
  "814141": {
    "pincode": "814141",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Jarmundi SO",
      "Amba Jarowadih BO",
      "Banwara BO",
      "Beldaha BO",
      "Bishanpur BO",
      "Dudhani BO",
      "P Jaratikar BO",
      "Raikinari BO"
    ]
  },
  "814142": {
    "pincode": "814142",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Jasidih SO",
      "Banka BO",
      "Dabar Gram BO",
      "Deopur BO",
      "Kenmankathi BO",
      "Madhopur BO"
    ]
  },
  "814143": {
    "pincode": "814143",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Kunda SO Deoghar",
      "AK Bad BO",
      "Baijukura BO",
      "Chanddih BO",
      "Tapoban BO"
    ]
  },
  "814144": {
    "pincode": "814144",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Bagnal BO",
      "Banskuli BO",
      "Bilkandi BO",
      "Brindabani BO",
      "Ghormala Hunja BO",
      "Jaitara BO",
      "Karikadar BO",
      "Manik Dih BO",
      "Ranibahal BO",
      "Sadipur BO",
      "Massanjore SO"
    ]
  },
  "814145": {
    "pincode": "814145",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Amarpur BO",
      "B Mahadeogarh BO",
      "Bhadwari BO",
      "Bouria BO",
      "Dhanbai BO",
      "Hasdiha BO",
      "Jamjori Birnia BO",
      "Kakania BO",
      "Kanjo BO",
      "Khutahari BO",
      "Khutan BO",
      "Kurumpahari BO",
      "Lakarbank BO",
      "Patgora BO",
      "Petsar BO",
      "Sultanatikar BO",
      "Thakur Nahar BO",
      "Nonihat SO"
    ]
  },
  "814146": {
    "pincode": "814146",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Ajnaridudhani BO",
      "Asna BO",
      "Baijnathpur BO",
      "Bansbutia BO",
      "Baragamharia BO",
      "Basaha BO",
      "Bhirabpur BO",
      "Bhutokoria BO",
      "Birajpur BO",
      "Chaglajore BO",
      "Chikania BO",
      "Dubrajpur BO",
      "Garapathar BO",
      "Garsara BO",
      "Golbandha BO",
      "Gundlidangal BO",
      "Jamua BO",
      "Jarwadi BO",
      "Karrasal BO",
      "Khairabani BO",
      "Kunjora BO",
      "Nachangaria BO",
      "Pargodih BO",
      "Pindari BO",
      "Sagarbanga BO",
      "Sarsa BO",
      "Palojori SO",
      "TDDumaria BO",
      "Talghara BO",
      "Tarajora BO"
    ]
  },
  "814147": {
    "pincode": "814147",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Pathergama SO",
      "Bandanbar BO",
      "Barmasiya BO",
      "Bhadharia BO",
      "Ghat Kuruwa BO",
      "Kero Bazar BO",
      "Kerwarbelsar BO",
      "Khariani BO",
      "Kusumghati BO",
      "Lahati BO",
      "Parspani BO",
      "Pipra BO",
      "Raja Bhitaa BO",
      "Tardiha BO"
    ]
  },
  "814148": {
    "pincode": "814148",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Raghunathpur SO Dumka",
      "Amjora BO",
      "Dumra BO",
      "Jai Haripur BO",
      "Kumirdaha BO",
      "Pathra BO",
      "Patjore BO",
      "Pratappur BO",
      "Rangalia BO",
      "Ranigram BO",
      "Sukjora BO",
      "Tangdaha BO"
    ]
  },
  "814149": {
    "pincode": "814149",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Sarath SO",
      "Bamandiha BO",
      "Charak Mara BO",
      "Choudri Nawadih BO",
      "Dumaria BO",
      "Gopibandh BO",
      "Jhagrahi BO",
      "Phulchuan BO",
      "Sabaijore BO"
    ]
  },
  "814150": {
    "pincode": "814150",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Sarwan SO",
      "Lakhoria BO",
      "Madhuban BO",
      "Manigarhi BO",
      "Nanhidih BO",
      "Paharidih BO",
      "Pahariya BO",
      "Paway BO",
      "Purjori BO",
      "Rakti BO",
      "Sonaraithari BO",
      "Tilakpur BO",
      "M Bsnsbutla BO"
    ]
  },
  "814151": {
    "pincode": "814151",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Saraiyahat SO",
      "Barkundi BO",
      "Chandubathan BO",
      "Chihutia BO",
      "Chilra BO",
      "Dhamni BO",
      "Dighi BO",
      "Gadijhopa BO",
      "Harokha Aswari BO",
      "Jokela BO",
      "Kendua BO",
      "M Chikaniya BO",
      "Nawadih BO",
      "Raoundhya BO",
      "Samaymatha BO"
    ]
  },
  "814152": {
    "pincode": "814152",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Rohani SO",
      "Baghmari BO",
      "Barkikharkhar BO",
      "Bhojpur BO",
      "Daluraidih BO",
      "Debipur BO",
      "Ghorlas BO",
      "K Banskola BO",
      "Koiridih BO",
      "Maniyarpur BO",
      "Punasi BO",
      "Ramudih BO"
    ]
  },
  "814153": {
    "pincode": "814153",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Poraiyahat SO",
      "Agiyamore BO",
      "Asarimadhuri BO",
      "Baghmara BO",
      "Burhi Kura BO",
      "Chatra BO",
      "Dama BO",
      "Damruhat BO",
      "Danrey Katchery BO",
      "Deodawr BO",
      "Ghanghrabandh BO",
      "Hariyari BO",
      "Kairadih BO",
      "Kathaun BO",
      "Latta BO",
      "Mohani BO",
      "Nawdiha BO",
      "P Barmasiya BO",
      "Padampur BO",
      "Pasai BO",
      "Pindra Hat BO",
      "Raghunathpur Lathibari BO",
      "Salaiya BO"
    ]
  },
  "814154": {
    "pincode": "814154",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Mahgama SO",
      "Anjana BO",
      "Bhanjpur BO",
      "Bharta Chak BO",
      "Dharmodih BO",
      "Dighi BO",
      "Ghoothi BO",
      "Hanwara BO",
      "Kasba BO",
      "Koila BO",
      "Kushmara BO",
      "Kushmil BO",
      "Logain BO",
      "Mal Bhandaridih BO",
      "Mohanpur BO",
      "Naraini BO",
      "Naya Nagar BO",
      "Nunajore BO"
    ]
  },
  "814155": {
    "pincode": "814155",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Azampakariya BO",
      "Basantrai SO",
      "Bisambharchak BO",
      "Biswaskhani BO",
      "K Parsa BO",
      "Kadma BO",
      "Kaitha BO",
      "Laxmipur Bhojichak BO",
      "Maheshtikri BO",
      "Maliachak BO",
      "Mokalchak BO",
      "Parariya BO",
      "Parua BO",
      "Rupni BO",
      "Sahapur Beldiha BO",
      "Samri BO",
      "Sanour BO"
    ]
  },
  "814156": {
    "pincode": "814156",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Sarouni Bazar SO",
      "Bans Jori BO",
      "Chandanahat BO",
      "China Dhab BO",
      "Ghatiyari BO",
      "Kurmichak BO",
      "Nunbatta BO",
      "Paharpur BO",
      "Sunder Pahari BO",
      "Sundmara BO"
    ]
  },
  "814157": {
    "pincode": "814157",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Dehijore SO",
      "Chulhiya BO",
      "Jamunia BO",
      "Jhillighat BO",
      "Kharagdiha BO",
      "Malhara BO"
    ]
  },
  "814158": {
    "pincode": "814158",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Guhiajori SO",
      "Amra Nawadih BO",
      "Bhurkunda BO",
      "Chainpur Kairabani BO",
      "Dhogariya BO",
      "K Madhuwadih BO",
      "Karbindha BO",
      "Machkicha BO",
      "Pakiriya BO",
      "Rajbandh BO",
      "Siltha BO",
      "Simalduma BO",
      "Tharihat BO"
    ]
  },
  "814160": {
    "pincode": "814160",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Meharma SO",
      "Adamdour BO",
      "Amour BO",
      "Bajitpur BO",
      "Belbadda BO",
      "Dariapur BO",
      "Dewanchak BO",
      "Dighisiwanpur BO",
      "Doi BO",
      "Gorikitta BO",
      "Kasbadudhanichak BO",
      "Lakarmara BO",
      "Marpa BO",
      "Pratappur BO",
      "Surni BO",
      "Tularambhuska BO"
    ]
  },
  "814165": {
    "pincode": "814165",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Lalmatia Colliery SO",
      "Barasimra BO",
      "Gajanda BO",
      "Gorhia BO",
      "Kamaldouri BO",
      "Lohandia Bazar BO",
      "Madhurai BO",
      "Sarbhanga BO",
      "Bapugram BO"
    ]
  },
  "814166": {
    "pincode": "814166",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Fatehpur SO Jamtara",
      "Babanbandhi BO",
      "Dhasania BO",
      "Gumro BO",
      "JKhairbani BO",
      "Jamjori BO",
      "K Bhoktadih BO",
      "Kairabani BO",
      "TKGram BO",
      "Tasaria BO"
    ]
  },
  "814167": {
    "pincode": "814167",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Ranga Masalia SO"
    ]
  },
  "815301": {
    "pincode": "815301",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Giridih HO",
      "Barganda SO",
      "Giridih Bazar SO",
      "Makatpur SO"
    ]
  },
  "815302": {
    "pincode": "815302",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Giridih  Town SO",
      "Baddiha BO",
      "Burhiadih BO",
      "Chaitadih BO",
      "Daridih BO",
      "Gadi Sermpur BO",
      "Motieda BO",
      "Serampur Colliery BO",
      "Sirsia BO",
      "Taratand BO",
      "Udnabad BO"
    ]
  },
  "815311": {
    "pincode": "815311",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Beniadih SO",
      "Akdonikala BO",
      "Karharbari BO",
      "Matrukha BO"
    ]
  },
  "815312": {
    "pincode": "815312",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Bengabad SO",
      "Ahilyapur BO",
      "Badwara BO",
      "Barkitand BO",
      "Budhudih BO",
      "Chapuadih BO",
      "Chhotki Kharagdiha BO",
      "Gadi Nawdiha BO",
      "Gadi Sirsia BO",
      "Gandey BO",
      "Khurchutta BO",
      "Luppi BO",
      "Maheshmunda BO",
      "Mundro BO",
      "Parwatpur BO",
      "Phulchi BO",
      "Phuljharia BO",
      "Purri BO",
      "Rata Bahiyar BO",
      "Siyatand BO",
      "Sonardih BO",
      "Tiklato BO",
      "Mahuwar B.O"
    ]
  },
  "815313": {
    "pincode": "815313",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Ganwan SO",
      "Bendro BO",
      "Birney BO",
      "Charki BO",
      "Kahuwai BO",
      "Malda BO",
      "Manjhney BO",
      "Pihra BO",
      "Sankh BO",
      "Serua B.O",
      "Gadar B.O"
    ]
  },
  "815314": {
    "pincode": "815314",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Kharagdiha SO",
      "Bairia BO",
      "Chatro BO",
      "Chikandih BO",
      "Deori BO",
      "Ghoranji BO",
      "Jagsemar BO",
      "Khariodih BO",
      "Machchali BO",
      "Mandro BO",
      "Manikbad BO",
      "Chahal B.O",
      "Salaidih B.O",
      "Bedodih B.O",
      "Tilakdih B.O"
    ]
  },
  "815315": {
    "pincode": "815315",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Mirzaganj SO",
      "Dhuraita BO",
      "Kurhobindo BO",
      "Pobi BO",
      "Sankho BO",
      "Lataki BO"
    ]
  },
  "815316": {
    "pincode": "815316",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Pachamba SO",
      "Bairiabad BO",
      "Bajto BO",
      "Balia BO",
      "Berhabad BO",
      "Dwarpahari BO",
      "Handadih BO",
      "Karma BO",
      "Leda BO",
      "Pesham BO",
      "Ranikhawa BO",
      "Senadoni BO",
      "Padarmania B.O"
    ]
  },
  "815317": {
    "pincode": "815317",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Tisri SO",
      "Bhandari BO",
      "Chandouri BO",
      "Gumgi BO",
      "Kathkoko BO",
      "Khijuri BO",
      "Kishutand BO",
      "Lokai BO",
      "Papilo BO",
      "Mansadih B.O",
      "Kharkhari B.O"
    ]
  },
  "815318": {
    "pincode": "815318",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Giridih Division",
    "offices": [
      "Jamua SO Giridh",
      "Balgo BO",
      "Bati BO",
      "Charghara BO",
      "Chittardih BO",
      "Chunglo BO",
      "Dumma BO",
      "Jeruadih BO",
      "Tara BO"
    ]
  },
  "815351": {
    "pincode": "815351",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Jamtara SO",
      "Asanchua BO",
      "Bagdaha BO",
      "Bena BO",
      "Bewa BO",
      "Bindapathar BO",
      "Chalna Dhobna BO",
      "Chapuria BO",
      "Chirudih Bakudih BO",
      "Chitra BO",
      "Dakhin Bahal BO",
      "Dhandra BO",
      "Geria BO",
      "Guhiyazori BO",
      "Gundali Pahari BO",
      "Harirakha BO",
      "Kanki BO",
      "Kasitar BO",
      "Koiri Jamua BO",
      "Ladhna BO",
      "Manghladih BO",
      "Mejhia BO",
      "Mohana Bank BO",
      "Noni BO",
      "Pabia BO",
      "Panjania BO",
      "Pattajori Kajra BO",
      "Sonbad BO",
      "Tilaki BO",
      "Tarni BO",
      "Jamtara Court SO"
    ]
  },
  "815352": {
    "pincode": "815352",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Karmatar SO",
      "Amdiha BO",
      "Amjora Koiridih BO",
      "Bagrudih BO",
      "Chainpur BO",
      "D Nayadih BO",
      "Dewalbari BO",
      "Gokula BO",
      "Kalajhariya BO",
      "Kurta BO",
      "Kurwa BO",
      "Mohanpur BO",
      "Narayanpur BO",
      "Pathrodih BO",
      "Ranitanr BO",
      "Sabanpur BO",
      "Sinduri BO",
      "Sitalpur BO"
    ]
  },
  "815353": {
    "pincode": "815353",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Madhupur SO",
      "Bagjora BO",
      "Bamangama BO",
      "Bara Chapra BO",
      "Barawan BO",
      "Burhai BO",
      "Charpa BO",
      "Dhamni BO",
      "Haripur Kolwa BO",
      "Hussainabad BO",
      "Jagdishpur BO",
      "Karanjo BO",
      "Kasathi BO",
      "Kushmaha BO",
      "Margomunda BO",
      "Ojhadih BO",
      "Patharda BO",
      "Pathrol BO",
      "Ramchandrapur BO",
      "Saptar BO",
      "Sirsa BO",
      "Upper Bahiyar BO",
      "Madhupur Bazar SO"
    ]
  },
  "815354": {
    "pincode": "815354",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Mihijam SO",
      "Chandra Dhipa BO",
      "Kelahi BO",
      "Kewatjali BO",
      "Sahardal BO",
      "Tarrah BO"
    ]
  },
  "815355": {
    "pincode": "815355",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Nala SO",
      "Afzalpur BO",
      "Baragholjore BO",
      "Debjore BO",
      "Kalipahari BO",
      "Karaiya BO",
      "Krishnapur BO",
      "Kuldangal BO",
      "Manihari BO",
      "Sagjoria BO",
      "Saraskunda BO",
      "Sitamurhi BO",
      "Tesjuria BO"
    ]
  },
  "815357": {
    "pincode": "815357",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Karogram SO",
      "Baramsoli BO",
      "Baratanr BO",
      "Baskupi BO",
      "Kukraha BO",
      "Madankatha BO",
      "Ranidih BO",
      "Siktiya BO"
    ]
  },
  "815359": {
    "pincode": "815359",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Kundahit SO",
      "Amba BO",
      "Babupur BO",
      "Bagdehri BO",
      "Charakmara BO",
      "Khajuree BO",
      "Laykapur BO",
      "Nagri BO",
      "Palajuri BO",
      "Satki BO",
      "Sudrakshipur BO",
      "Tilabad BO"
    ]
  },
  "816101": {
    "pincode": "816101",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Santhal Parganas Division",
    "offices": [
      "Barharwa SO Sahibganj",
      "Aglio BO",
      "Atapur BO",
      "Bakudih BO",
      "Baradighi BO",
      "Begamganj BO",
      "Chandsaher BO",
      "Ganeshpur BO",
      "Goalkhor BO",
      "Ishlampur BO",
      "Jampur BO",
      "Kankjol BO",
      "Kathalbari BO",
      "Kelabari BO",
      "Masna BO",
      "Palasbona BO",
      "Radhanager BO",
      "Ranigram BO",
      "Reshore BO",
      "Sridhardira BO",
      "Srikund BO"
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
