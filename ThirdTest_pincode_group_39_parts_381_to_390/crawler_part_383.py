"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 383 / 400
================================================================================
- Group: ThirdTest_pincode_group_39_parts_381_to_390
- Assigned PIN Codes: 48 (Range: 822114 to 824205)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_383.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_383.csv & .json
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

PART_ID = "part_383"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-383] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "822114",
  "822115",
  "822116",
  "822117",
  "822118",
  "822119",
  "822120",
  "822121",
  "822122",
  "822123",
  "822124",
  "822125",
  "822126",
  "822128",
  "822129",
  "822131",
  "822132",
  "822133",
  "822134",
  "823001",
  "823002",
  "823003",
  "823004",
  "823005",
  "823311",
  "824101",
  "824102",
  "824103",
  "824111",
  "824112",
  "824113",
  "824114",
  "824115",
  "824116",
  "824118",
  "824120",
  "824121",
  "824122",
  "824123",
  "824124",
  "824125",
  "824127",
  "824129",
  "824143",
  "824201",
  "824202",
  "824203",
  "824205"
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
  "822114": {
    "pincode": "822114",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Garhwa SO",
      "Adar BO",
      "Anraj Nawadih BO",
      "Belhara BO",
      "Chinia BO",
      "Doll BO",
      "Dumariya BO",
      "Hoor BO",
      "Kalyanpur BO",
      "Nawada BO",
      "Okhargara BO",
      "Peska BO",
      "Ranicheri BO",
      "Ranka bauliya BO",
      "Ranpura BO",
      "Soh BO",
      "Tildag BO",
      "Achla",
      "Chama",
      "Chhattarpur",
      "Chiraunjia",
      "Dubey Marhatiya",
      "Jata",
      "Jhotar",
      "Mahuliya",
      "Obra",
      "Parihara",
      "Pharathiya",
      "Pipra",
      "Ranka",
      "Ursugi"
    ]
  },
  "822115": {
    "pincode": "822115",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Haidar Nagar SO",
      "Bilashpur BO",
      "Chaukri BO",
      "Kabrakhurd BO",
      "Karimandih BO",
      "Kosiara BO",
      "Kukahi BO",
      "Pansa BO",
      "Sundipur BO",
      "Babhandih",
      "Imamnagar Barewa",
      "Sadeya"
    ]
  },
  "822116": {
    "pincode": "822116",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Japla SO",
      "Alinagar BO",
      "Barahi BO",
      "Dangwar BO",
      "DaruaBeni BO",
      "Joga jamdiha BO",
      "Jhargara BO",
      "Kajrat Nawadih BO",
      "Kamgarpur BO",
      "Kusha BO",
      "Latheya BO",
      "Mahuari BO",
      "Poldihjagdishpur BO",
      "Sabano BO",
      "Utari Road BO",
      "Badepur",
      "Bairaon",
      "jamua",
      "Kalapahad",
      "Karkatta",
      "Kosi",
      "Kurmipur",
      "Lotaniya"
    ]
  },
  "822117": {
    "pincode": "822117",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Japla C F SO"
    ]
  },
  "822118": {
    "pincode": "822118",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Lesliganj SO",
      "Ashehar BO",
      "Bansdih BO",
      "Basaura BO",
      "Champi BO",
      "Dhangaon BO",
      "Phulang BO",
      "Gentha BO",
      "Gurha BO",
      "Gurua BO",
      "Kazi Pakri BO",
      "Kundri BO",
      "Manjhigaon BO",
      "Pathakpagar BO",
      "Rajwadih BO",
      "Sagalim BO",
      "Sangbar BO",
      "Sildiliya BO",
      "Tarhasi BO",
      "Thakuraidabra BO",
      "Arka",
      "Darudih",
      "Ghotua",
      "Goindi",
      "Hartua",
      "Jamune",
      "Juru",
      "Kotkhas",
      "Kurain Patra",
      "Pipra Khurd",
      "Rajhara",
      "Selari",
      "Tariya",
      "Udaipura"
    ]
  },
  "822119": {
    "pincode": "822119",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Mahuwadand SO",
      "Arahans BO",
      "Auxi BO",
      "Barahi BO",
      "Chatakpur BO",
      "Durup BO",
      "Hami BO",
      "Orsa BO",
      "Rajdanda BO",
      "Parhatoli B.O",
      "Chainpur B.O",
      "Champa B.O",
      "Rengai B.O"
    ]
  },
  "822120": {
    "pincode": "822120",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Mohammad Ganj SO",
      "Belhath BO",
      "Goradih BO",
      "Hariharpur BO",
      "Jainagra BO",
      "Kadalkurmi BO",
      "Kandi BO",
      "Kushha BO",
      "Lamarikala BO",
      "Manjhiaon BO",
      "Morway BO",
      "Satbahini BO",
      "Sonaura BO",
      "Shivpur BO",
      "Chataniya",
      "Ranadih",
      "Kolhua Sonbarsa",
      "Latpauri"
    ]
  },
  "822121": {
    "pincode": "822121",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Nagar Utari SO",
      "Adhaura BO",
      "Amba Khorya  BO",
      "Bhojpur BO",
      "Birbal BO",
      "Chitbishram BO",
      "Dhurki BO",
      "Halwanta Kala BO",
      "Jamui BO",
      "Katharkala BO",
      "Mahdeiya BO",
      "Ahirpurwa",
      "Bilaspur",
      "Hetarkala",
      "Checharia",
      "Garbandh",
      "Jangipur"
    ]
  },
  "822122": {
    "pincode": "822122",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Panki SO Palamau",
      "Ambabar BO",
      "Baliari BO",
      "Champi Kala BO",
      "Dandar Kala BO",
      "Dhub BO",
      "Dwarika BO",
      "Kasmar BO",
      "Konwai BO",
      "Loharsi BO",
      "Tal BO",
      "Tetrain BO",
      "Udaipur BO",
      "Hotai",
      "Hurlaung",
      "Karar",
      "Kelwa",
      "Maran",
      "Naudiha",
      "Nawadih",
      "Nuru",
      "Pagar Khurd",
      "Pakariya",
      "Panki West",
      "Ratanpur",
      "Sunri"
    ]
  },
  "822123": {
    "pincode": "822123",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Baidakala BO",
      "Chetma BO",
      "Dipauwa BO",
      "Dulhi BO",
      "Gahar Pathara BO",
      "Chak BO",
      "Kankekala BO",
      "Kishunpur BO",
      "Lami Patra BO",
      "Loinga BO",
      "Manatu BO",
      "Patan SO Palamau",
      "Nawa Jaipur BO",
      "Naudiha BO",
      "Padma BO",
      "Palhe Kala BO",
      "Pandeypura BO",
      "Roll BO",
      "Sirma BO",
      "Chhechhauri",
      "Dumri",
      "Janghasi",
      "Kajri",
      "Maghouli",
      "Mahulia",
      "Murma",
      "Naudiha-2",
      "Pachkeria",
      "Rangeya",
      "Saguna",
      "Sataua",
      "Semri",
      "Silidliya Khurd",
      "Sole",
      "Suntha",
      "Utaki"
    ]
  },
  "822124": {
    "pincode": "822124",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Rehala SO",
      "Basna BO",
      "Belchampa BO",
      "Bhandar BO",
      "Bhikhahi BO",
      "Danda BO",
      "Gari BO",
      "Kadhwan BO",
      "Kanda BO",
      "Ketat Kala BO",
      "Lalgarh BO",
      "Nawgarha BO",
      "Pandu BO",
      "Rajhara Colliery BO",
      "Sangrahekala BO",
      "Sigsigi BO",
      "Tolera BO",
      "Pandwa",
      "Radba",
      "Tukbera",
      "Guri"
    ]
  },
  "822125": {
    "pincode": "822125",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Ranka Raj SO",
      "Bairiya BO",
      "Bandu BO",
      "Bargarh BO",
      "Godarmana BO",
      "Kanjia BO",
      "Karso BO",
      "Khajutia Barwadih BO",
      "Nawa Bhandaria BO",
      "Paraswar BO",
      "Ramkanda BO",
      "SEWADIH BO",
      "Sribishrampur BO",
      "Tamge Kala BO",
      "Udaipur BO",
      "Baligarh BO",
      "Barwadih",
      "Beta",
      "Bulka",
      "Chete",
      "Dudhwal",
      "Bahahara",
      "Phakiradih",
      "Harhe",
      "Janewa",
      "Kachanpur",
      "Katra",
      "Khapro",
      "Khardiha",
      "Madgari (Chapia)",
      "Madgari (Kanjea)",
      "Manpur",
      "Raksi",
      "Sondag",
      "Tehri"
    ]
  },
  "822126": {
    "pincode": "822126",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Satbarwa SO",
      "Bakoriya BO",
      "Barwaiya BO",
      "Dhawadih BO",
      "Jhabar BO",
      "Khamdih BO",
      "Manika BO",
      "Matlong BO",
      "Palheya BO",
      "Polpol BO",
      "Ranki Kala BO",
      "Sikki Kala BO",
      "Bandua",
      "Bari",
      "Bishunbandh",
      "Bohita",
      "Donki",
      "Dulsulma",
      "Dundu",
      "Jamho",
      "Lahlahe",
      "Namudag",
      "Ponchi",
      "Rebaratu",
      "Sinjo"
    ]
  },
  "822128": {
    "pincode": "822128",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Ramna SO Garhwa",
      "Bhagodih BO",
      "Bulka BO",
      "Gamharia BO",
      "Ganiarikala BO",
      "Kocheya BO",
      "Marwania BO",
      "Pipri Kala BO",
      "Rohila BO",
      "Silidag BO",
      "Sonehara BO",
      "Tandwa BO",
      "Tatidiri BO",
      "Bahiyarkhurd",
      "Bisunpura",
      "HardagKalan",
      "Patihari",
      "Sarang"
    ]
  },
  "822129": {
    "pincode": "822129",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Bhawnathpur TownShip SO"
    ]
  },
  "822131": {
    "pincode": "822131",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Harihar Ganj SO Palamau",
      "Halka BO",
      "Kurhat Kataiya BO",
      "Satgawa BO",
      "Kulahia",
      "Semarwar"
    ]
  },
  "822132": {
    "pincode": "822132",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Bishrampur SO Palamau",
      "Tisibar BO",
      "Gharatia BO",
      "Kutmu BO",
      "Murma Kala BO",
      "Ratnag BO",
      "Dihariya BO",
      "Kajru Khurd BO",
      "Dalakala",
      "Fulia",
      "Ghasidag",
      "KajruKala",
      "Sildilli",
      "Mahudand"
    ]
  },
  "822133": {
    "pincode": "822133",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Meral SO",
      "Lagma BO",
      "Bana BO",
      "Jarhi BO",
      "Deogana BO",
      "Hasandag BO",
      "Banka BO",
      "Latdag BO",
      "Dandai BO",
      "Raro BO",
      "Balekhar BO",
      "Bahiyarkala",
      "Biktam",
      "Karke",
      "Lawahikalan",
      "Meral East",
      "Ohargara West",
      "Padua",
      "Khoridih",
      "Sangbaria",
      "Tasrar",
      "Tenar",
      "Tisartetuka"
    ]
  },
  "822134": {
    "pincode": "822134",
    "circle": "Jharkhand Circle",
    "region": "Ranchi Region",
    "division": "Palamau Division",
    "offices": [
      "Manjhiaon SO",
      "Karuie BO",
      "Ataula BO",
      "Harigawan BO",
      "Dwandkara BO",
      "Bardiha BO",
      "Karamdih",
      "Kharsota",
      "Sukhandi",
      "Chutiya",
      "Jatro Banjari",
      "Salga",
      "Talasbaria"
    ]
  },
  "823001": {
    "pincode": "823001",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Gaya HO",
      "APColony SO Gaya",
      "Chand Chourah SO",
      "Chandouti SO",
      "Chowk SO",
      "Civil Lines SO Gaya",
      "Gaya DB SO",
      "Gaya Jail SO",
      "Gewal Bigha SO",
      "Karimganj SO",
      "New Godown SO",
      "Purani Godown SO"
    ]
  },
  "823002": {
    "pincode": "823002",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Gaya RS SO",
      "Churi BO",
      "Kesru Dharampur BO",
      "Kujapi BO",
      "Bairagi SO",
      "Delha SO",
      "Kharkhura SO"
    ]
  },
  "823003": {
    "pincode": "823003",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Buniyad Ganj SO",
      "Alipur BO",
      "Baragandhar BO",
      "Bhadeji BO",
      "Bhoremirganj BO",
      "Bijubigha BO",
      "Bithosarif BO",
      "Budhgere BO",
      "Chhatubagh BO",
      "Khanjahapur BO",
      "Lodipur BO",
      "Maksudpur BO",
      "Manpur BO",
      "Nagariyawan BO",
      "Nauranga BO",
      "Rasalpur BO",
      "Sohaipur BO",
      "Sondhi BO",
      "Durga Asthan SO"
    ]
  },
  "823004": {
    "pincode": "823004",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Civil Aerodram SO",
      "Bagadaha BO",
      "Jamri BO",
      "Pandey Parasawan BO"
    ]
  },
  "823005": {
    "pincode": "823005",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Asc CentreN Gaya SO"
    ]
  },
  "823311": {
    "pincode": "823311",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Atri SO",
      "Ayer BO",
      "Bairka BO",
      "Bandimaniyara BO",
      "Bathani BO",
      "Birnoi BO",
      "Chiriyawan BO",
      "Jethian BO",
      "Khukri BO",
      "Punar BO",
      "Jamunapur Noudiha BO",
      "Patharkatti BO",
      "Piyar BO",
      "SMoulanagar BO",
      "Saren BO",
      "Sarvahada BO",
      "Telari BO",
      "Tetar BO",
      "Teusa Bandhubigha BO",
      "DHUSARI",
      "GEHLORE"
    ]
  },
  "824101": {
    "pincode": "824101",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Aurangabad BH HO",
      "Aurangabad Kutchehry SO"
    ]
  },
  "824102": {
    "pincode": "824102",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "S S College SO",
      "Bijauli BO",
      "Chauria BO",
      "G Kharma BO",
      "Ibrahimpur BO",
      "Ithat BO",
      "Jaipur BO",
      "Jogia BO",
      "Kanbehari BO",
      "Karma Bhagwan BO",
      "Khadiha BO",
      "Kunda BO",
      "Manjurahi BO",
      "Naugarh BO",
      "R B Nagar BO",
      "Rajpur BO",
      "Risiap BO",
      "Riyasat Mali BO",
      "Riyasat Pawai BO",
      "Silar BO",
      "Sori BO",
      "Sunurganj BO"
    ]
  },
  "824103": {
    "pincode": "824103",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "War SO",
      "Akhoini Regania BO",
      "Baligaon BO",
      "Chain BO",
      "Choukhra BO",
      "Dadhapi BO",
      "Gardi BO",
      "Ghoshta BO",
      "Kapasia BO",
      "Khaira Manjhouli BO",
      "Kona BO",
      "P Sikandarpur BO",
      "Paharpura BO",
      "Poiwan BO",
      "Rajoi BO",
      "Teldiha BO",
      "Udhampur BO"
    ]
  },
  "824111": {
    "pincode": "824111",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Amba SO",
      "Ballia BO",
      "Bedauli BO",
      "CDhongra BO",
      "Deshpur BO",
      "Dumra BO",
      "Gheura BO",
      "Mahsu BO",
      "Matpa BO",
      "Pandaria BO",
      "Parta BO"
    ]
  },
  "824112": {
    "pincode": "824112",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Barun SO AurangabadBH",
      "Charan BO",
      "Jagatpur BO",
      "Jagdishpur BO",
      "Jhumardihra BO",
      "Kashipur Tetaria BO",
      "Urdina BO",
      "R Pithanua BO",
      "R B Bigha BO",
      "Saduri Karma BO",
      "Satuahi BO",
      "Siris BO",
      "Son Nagar BO",
      "Tengra BO"
    ]
  },
  "824113": {
    "pincode": "824113",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Bhakharua SO",
      "Akauni BO",
      "Aranda BO",
      "Chanda BO",
      "Gaini BO",
      "Khairadip BO",
      "Pilchhi BO",
      "Tara BO",
      "Tarari BO"
    ]
  },
  "824114": {
    "pincode": "824114",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Deohara SO",
      "Barpa BO",
      "Jaitpur BO",
      "Malhara BO",
      "Munjhar BO",
      "Shekhpura BO"
    ]
  },
  "824115": {
    "pincode": "824115",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Sihari SO",
      "Angrahi BO",
      "Dhamani BO",
      "Jhinguri BO",
      "Koilwan BO",
      "Sonhathoo BO"
    ]
  },
  "824116": {
    "pincode": "824116",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Anti SO",
      "Angra BO",
      "Bham BO",
      "Darma BO",
      "Jaitiya BO",
      "Kathautia BO"
    ]
  },
  "824118": {
    "pincode": "824118",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "Gaya Division",
    "offices": [
      "Guraru Mills SO",
      "Burman BO",
      "Budhoul BO",
      "Daboor BO",
      "Deokali BO",
      "Dighi BO",
      "Diha BO",
      "Dushadh Bigha BO",
      "Ghatera BO",
      "Kanousi BO",
      "Kerki BO",
      "Konchi BO",
      "Korap BO",
      "Kormathu BO",
      "Malpa BO",
      "Manjhiyawan BO",
      "Mathurapur BO",
      "Paharabali BO",
      "Tineri BO"
    ]
  },
  "824120": {
    "pincode": "824120",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Haspura SO",
      "Apiroo BO",
      "Baghoi BO",
      "Bantara BO",
      "Chaurahi BO",
      "Dindir BO",
      "Dumra BO",
      "Entwan BO",
      "G Kenap BO",
      "Hathiara BO",
      "Jalpura BO",
      "Khuthan BO",
      "Purhara BO"
    ]
  },
  "824121": {
    "pincode": "824121",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Jamhor SO",
      "Deoria Kala BO",
      "Phesar BO",
      "Bhartholi BO",
      "Karsawan BO",
      "Panrawan BO",
      "Pauthu BO",
      "Pokhraha BO",
      "Ramchandra Nagar BO",
      "Sarsawli BO",
      "Unthu BO",
      "Gijna BO"
    ]
  },
  "824122": {
    "pincode": "824122",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Jakhim SO",
      "Barahi BO",
      "Bhadwa Bazar BO",
      "Dosma BO",
      "Indrar BO",
      "Karsara BO",
      "Kurwan BO",
      "Lahasa BO",
      "Latta BO",
      "Lukka BO",
      "Molbiganj Pothu BO",
      "Temura BO",
      "Itar BO"
    ]
  },
  "824123": {
    "pincode": "824123",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Kutumba SO",
      "Ankupa BO",
      "K P Tendua BO",
      "Khaira Rajpur BO",
      "Narendra Khap BO",
      "Pipra Bagahi BO",
      "Turta BO"
    ]
  },
  "824124": {
    "pincode": "824124",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Obra SO AurangabadBH",
      "Amilauna BO",
      "B Sagarpur BO",
      "Barauli BO",
      "Bel BO",
      "Bharub BO",
      "Chechari BO",
      "Dhangain BO",
      "Dihara BO",
      "Jai Govind Nagar BO",
      "Kaithi BO",
      "Kara BO",
      "Khudwan BO",
      "Kurhwan BO",
      "Malwan BO",
      "Narainpur BO",
      "P Babhandiha BO",
      "Ramnagar BO",
      "Sananpura BO",
      "Sunbarasa BO",
      "Tejpura BO"
    ]
  },
  "824125": {
    "pincode": "824125",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Rafiganj SO",
      "Arthua BO",
      "Baksibigha BO",
      "Bishambharpur BO",
      "Chiraila BO",
      "Dugul BO",
      "Gordiha BO",
      "Kasma BO",
      "Kerap BO",
      "Nima Chaturbhuj BO",
      "Panti BO",
      "Pogar BO",
      "Ranidih BO",
      "Sahukarma BO",
      "Sihuli Khaira BO",
      "Sihuli BO",
      "Simla BO",
      "Charkanwan BO",
      "Chubra BO",
      "Sarawak BO"
    ]
  },
  "824127": {
    "pincode": "824127",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Kaler SO",
      "Belaon BO",
      "Belsar BO",
      "Chauri BO",
      "Hridaychak BO",
      "Manpur Chanda BO",
      "Sohsa BO"
    ]
  },
  "824129": {
    "pincode": "824129",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Rukundi Jhikatia SO",
      "Baxar BO",
      "Dadar BO",
      "Dihuri Ekauni BO",
      "Karma Pandey BO",
      "Sandya BO"
    ]
  },
  "824143": {
    "pincode": "824143",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Daudnagar SO",
      "Agnoor BO",
      "Amauna BO",
      "Anchha BO",
      "Arai BO",
      "Dhnaoh Bigha BO",
      "Fort Daudnagar BO",
      "H Bigha BO",
      "Jamuawan BO",
      "Shamshernagar BO",
      "Sansa BO",
      "Tarar BO"
    ]
  },
  "824201": {
    "pincode": "824201",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Barachatti SO",
      "Bhaluachatti BO",
      "Binda BO",
      "Bumuar BO",
      "Dongra BO",
      "Dewania BO",
      "Dhamna BO",
      "Gopalkera BO",
      "Jaigir BO",
      "Lahathua BO",
      "Patluka BO",
      "Rondawan BO",
      "Sharwankhas BO"
    ]
  },
  "824202": {
    "pincode": "824202",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Deo SO",
      "Ararua BO",
      "Bahuara BO",
      "Baluganj BO",
      "Banua BO",
      "Bedhani BO",
      "Belsara BO",
      "Bishunpur Chatti BO",
      "Dadhpa BO",
      "Dhibra BO",
      "Dumari BO",
      "Dumri Belwan BO",
      "Erki BO",
      "Ketaki BO",
      "Malhara BO",
      "Manika BO",
      "Niyamatpur BO",
      "Pachaukhar BO",
      "Sargawan BO",
      "Singhana BO",
      "Silar BO",
      "Kataiya"
    ]
  },
  "824203": {
    "pincode": "824203",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Goh SO",
      "Akauna BO",
      "Amari BO",
      "Ankuri BO",
      "Belawarish BO",
      "Budhai BO",
      "Darha BO",
      "Darwan BO",
      "Fag BO",
      "Gamhari BO",
      "Gorkatti BO",
      "Kaithi Siro BO",
      "Malhad BO",
      "Munjhara BO",
      "Sakardiha BO",
      "Singhari BO",
      "Tayap BO",
      "Turk Telpa BO",
      "Uphara BO"
    ]
  },
  "824205": {
    "pincode": "824205",
    "circle": "Bihar Circle",
    "region": "Patna HQ Region",
    "division": "AurangabadBihar Division",
    "offices": [
      "Gurua SO",
      "Balia BO",
      "Bharaunda BO",
      "Chansi BO",
      "Duba BO",
      "Ismailpur BO",
      "Pakri BO",
      "Sahuberma BO"
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
