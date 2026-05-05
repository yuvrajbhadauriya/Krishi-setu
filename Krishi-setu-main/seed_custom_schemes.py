import os
import sqlite3
import json
from pathlib import Path
from scraper import SchemeDatabase

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "output" / "db" / "agri_schemes.db"

CUSTOM_SCHEMES = [
    {
        "scheme_name": "Sub-Mission on Agricultural Mechanisation (SMAM)",
        "short_name": "SMAM",
        "scheme_type": "subsidy",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2014",
        "objective": "To increase the reach of farm mechanization to small and marginal farmers and to the regions where availability of farm power is low, offset adverse 'economies of scale', and provide 'tractor ke liye paisa'.",
        "target_beneficiaries": "Small, marginal, and all categories of farmers",
        "eligibility_rules": [
            "Must be a practicing farmer.",
            "Must have valid land records in their name.",
            "Should not have availed subsidy for the same equipment/tractor under any other scheme in the last few years."
        ],
        "exclusions": [
            "Non-agricultural entities."
        ],
        "benefit_amount": "Upto 50% to 80% subsidy on purchase of agricultural machinery including tractors.",
        "premium_or_cost": "Farmer contributes the remaining percentage of the equipment cost.",
        "requirements": [
            "Aadhaar card",
            "Land records (Patta/Khasra/Khatauni)",
            "Bank passbook",
            "Passport size photo",
            "Quotation of the machinery from an approved vendor"
        ],
        "application_process": "Apply online via the direct benefit transfer (DBT) portal of the respective state government.",
        "official_website": "https://agrimachinery.nic.in/",
        "key_facts": [
            "Provides financial assistance for purchasing tractors.",
            "Encourages Custom Hiring Centres (CHCs)."
        ],
        "keywords_hindi": ["ट्रैक्टर", "सब्सिडी", "मशीनीकरण"]
    },
    {
        "scheme_name": "Pradhan Mantri Fasal Bima Yojana (PMFBY)",
        "short_name": "PMFBY",
        "scheme_type": "insurance",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2016",
        "objective": "To provide insurance coverage and financial support to the farmers in the event of failure of any of the notified crop as a result of natural calamities, pests & diseases. Includes beema for wheat and flood damaged crop compensation.",
        "target_beneficiaries": "All farmers growing notified crops in a notified area during the season who have insurable interest in the crop.",
        "eligibility_rules": [
            "Grow a notified crop (like wheat, paddy) in a notified area.",
            "Must have valid land record or tenancy agreement.",
            "Loanee farmers are automatically covered, non-loanee farmers can voluntarily opt in."
        ],
        "exclusions": [
            "Preventable risks like theft, malicious damage.",
            "War and nuclear risks."
        ],
        "benefit_amount": "Compensation based on the shortfall in yield against the threshold yield.",
        "premium_or_cost": "Max 2% for Kharif, 1.5% for Rabi (including wheat), and 5% for commercial/horticultural crops.",
        "requirements": [
            "Aadhaar card",
            "Land record details (Khasra/Khatauni)",
            "Sowing certificate",
            "Bank account details"
        ],
        "application_process": "Through banks (for loanee farmers), CSCs, or PMFBY portal directly.",
        "official_website": "https://pmfby.gov.in/",
        "key_facts": [
            "Lowest ever premium rates for farmers.",
            "Use of technology like smartphones and drones for quick assessment."
        ],
        "keywords_hindi": ["फसल", "बीमा", "बाढ़"]
    },
    {
        "scheme_name": "Pradhan Mantri Krishi Sinchayee Yojana - Per Drop More Crop",
        "short_name": "PMKSY-PDMC",
        "scheme_type": "subsidy",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2015",
        "objective": "Focuses on enhancing water use efficiency at farm level through Micro Irrigation technologies viz. Drip and Sprinkler irrigation systems (drip irrigation subsidy).",
        "target_beneficiaries": "All farmers, with special focus on small & marginal farmers.",
        "eligibility_rules": [
            "Farmer must own agricultural land.",
            "Water source must be available (well, borewell, canal, etc.).",
            "Preference given to women and SC/ST farmers."
        ],
        "exclusions": [],
        "benefit_amount": "55% subsidy for small and marginal farmers; 45% for other farmers on micro-irrigation systems.",
        "premium_or_cost": "Remaining cost borne by the farmer.",
        "requirements": [
            "Aadhaar card",
            "Land document (7/12 extract or equivalent)",
            "Bank account details",
            "Water source proof"
        ],
        "application_process": "Apply through State Agriculture/Horticulture Department portals.",
        "official_website": "https://pmksy.gov.in/",
        "key_facts": [
            "Promotes drip and sprinkler irrigation.",
            "Saves water and increases crop yield."
        ],
        "keywords_hindi": ["सिंचाई", "ड्रिप", "सब्सिडी"]
    },
    {
        "scheme_name": "Sub-Mission on Seeds and Planting Material (SMSP)",
        "short_name": "SMSP",
        "scheme_type": "subsidy",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2014",
        "objective": "To promote production and multiplication of quality seeds, and provide seed money for farming and seed variety subsidy schemes.",
        "target_beneficiaries": "Farmers, Seed producing agencies, State governments.",
        "eligibility_rules": [
            "Practicing farmer with land.",
            "Willingness to participate in Seed Village Programme."
        ],
        "exclusions": [],
        "benefit_amount": "Financial assistance ranging from 50% to 75% for seed distribution.",
        "premium_or_cost": "Varies by specific intervention.",
        "requirements": [
            "Aadhaar card",
            "Land records",
            "Bank passbook"
        ],
        "application_process": "Through State Department of Agriculture / Gram Panchayat.",
        "official_website": "https://seednet.gov.in/",
        "key_facts": [
            "Supports Seed Village Programme.",
            "Ensures availability of certified seeds."
        ],
        "keywords_hindi": ["बीज", "सब्सिडी"]
    },
    {
        "scheme_name": "Kisan Credit Card (KCC) Scheme",
        "short_name": "KCC",
        "scheme_type": "credit",
        "nodal_ministry": "Ministry of Finance / Ministry of Agriculture",
        "launch_year": "1998",
        "objective": "To provide adequate and timely credit support from the banking system under a single window with flexible and simplified procedure to the farmers (kisan credit card apply).",
        "target_beneficiaries": "Farmers - Individuals / Joint borrowers who are owner cultivators; Tenant Farmers, Oral Lessees & Share Croppers.",
        "eligibility_rules": [
            "Must be a farmer (owner, tenant, or sharecropper).",
            "Self Help Groups (SHGs) or Joint Liability Groups (JLGs) of farmers."
        ],
        "exclusions": [
            "Defaulters of any previous bank loans."
        ],
        "benefit_amount": "Flexible credit limit based on crop area, scale of finance, and post-harvest expenses. Interest subvention available.",
        "premium_or_cost": "Standard interest rates; effectively 4% per annum if repaid promptly.",
        "requirements": [
            "Duly filled application form",
            "Identity Proof (Aadhaar, PAN, Voter ID)",
            "Address Proof",
            "Land documents"
        ],
        "application_process": "Can be applied through commercial banks, RRBs, Small Finance Banks, and Cooperatives, or via PM Kisan portal.",
        "official_website": "https://pmkisan.gov.in/",
        "key_facts": [
            "Valid for 5 years subject to annual review.",
            "Includes ATM enabled RuPay Card."
        ],
        "keywords_hindi": ["किसान क्रेडिट कार्ड", "ऋण", "लोन"]
    },
    {
        "scheme_name": "Agricultural Technology Management Agency (ATMA) Scheme",
        "short_name": "ATMA",
        "scheme_type": "other",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2005",
        "objective": "To support State extension programmes for extension reforms. It includes capacity building programmes and free training for farmers.",
        "target_beneficiaries": "Farmers, Farm Women, Farmer Interest Groups (FIGs).",
        "eligibility_rules": [
            "Farmers willing to adopt new agricultural technologies.",
            "Participation in local farmer groups is encouraged."
        ],
        "exclusions": [],
        "benefit_amount": "Free training, exposure visits, demonstrations, and capacity building.",
        "premium_or_cost": "Nil for farmers.",
        "requirements": [
            "Aadhaar card",
            "Registration with local ATMA office / block technology manager."
        ],
        "application_process": "Contact Block Technology Manager (BTM) or Krishi Vigyan Kendra (KVK).",
        "official_website": "https://extensionreforms.dacnet.nic.in/",
        "key_facts": [
            "Promotes decentralized, farmer-driven agricultural extension.",
            "Provides free training."
        ],
        "keywords_hindi": ["प्रशिक्षण", "मुफ्त", "तकनीक"]
    },
    {
        "scheme_name": "Pradhan Mantri Kisan Urja Suraksha evam Utthaan Mahabhiyan (PM-KUSUM)",
        "short_name": "PM-KUSUM",
        "scheme_type": "subsidy",
        "nodal_ministry": "Ministry of New and Renewable Energy",
        "launch_year": "2019",
        "objective": "Solar pump yojana. To provide energy security along with financial and water security to farmers by subsidizing standalone solar agriculture pumps.",
        "target_beneficiaries": "Individual farmers, Water User Associations, Farmer Producer Organisations (FPOs).",
        "eligibility_rules": [
            "Farmer should have a valid water source.",
            "Must have agricultural land.",
            "For grid-connected pumps, existing grid connection is required."
        ],
        "exclusions": [],
        "benefit_amount": "Up to 60% subsidy on the cost of the solar pump (30% Central, 30% State).",
        "premium_or_cost": "Farmer contribution is 40% (can be financed via bank loan up to 30%).",
        "requirements": [
            "Aadhaar card",
            "Land documents",
            "Bank details",
            "Passport size photo"
        ],
        "application_process": "Apply online through State Nodal Agency (SNA) portal for PM-KUSUM.",
        "official_website": "https://pmkusum.mnre.gov.in/",
        "key_facts": [
            "Reduces reliance on diesel pumps.",
            "Farmers can sell surplus power to the grid."
        ],
        "keywords_hindi": ["सौर ऊर्जा", "पंप", "कुसुम"]
    },
    {
        "scheme_name": "Paramparagat Krishi Vikas Yojana (PKVY)",
        "short_name": "PKVY",
        "scheme_type": "organic_farming",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2015",
        "objective": "To promote organic farming and chemical-free agriculture through the cluster approach and PGS certification. Provides an organic farming grant.",
        "target_beneficiaries": "Farmers forming clusters for organic farming.",
        "eligibility_rules": [
            "Must be part of a cluster (usually 50 or more farmers having 50 acres of land collectively).",
            "Commitment to adopt organic farming practices."
        ],
        "exclusions": [
            "Farmers continuing heavy chemical pesticide usage."
        ],
        "benefit_amount": "Financial assistance of Rs. 50,000 per hectare for 3 years, out of which Rs. 31,000 is given directly via DBT.",
        "premium_or_cost": "Nil.",
        "requirements": [
            "Aadhaar card",
            "Land records",
            "Cluster formation documents"
        ],
        "application_process": "Through State Agriculture Department under the cluster approach.",
        "official_website": "https://pgsindia-ncof.gov.in/",
        "key_facts": [
            "Promotes Participatory Guarantee System (PGS) certification.",
            "Reduces dependence on fertilizers."
        ],
        "keywords_hindi": ["जैविक", "कृषि", "अनुदान"]
    },
    {
        "scheme_name": "Crop Residue Management Scheme (CRM)",
        "short_name": "CRM",
        "scheme_type": "subsidy",
        "nodal_ministry": "Ministry of Agriculture and Farmers Welfare",
        "launch_year": "2018",
        "objective": "To address air pollution by providing farm mechanisation subsidy for in-situ management of crop residue, including land levelling equipment and super seeders.",
        "target_beneficiaries": "Farmers, Cooperative Societies, FPOs, and Panchayats in Punjab, Haryana, UP, and Delhi.",
        "eligibility_rules": [
            "Must be located in the targeted states.",
            "Must possess agricultural land and required tractor capacity."
        ],
        "exclusions": [],
        "benefit_amount": "50% subsidy for individual farmers, and 80% subsidy for Custom Hiring Centres (CHCs).",
        "premium_or_cost": "Remaining cost to be borne by the beneficiary.",
        "requirements": [
            "Aadhaar card",
            "Tractor RC",
            "Land records",
            "Bank account"
        ],
        "application_process": "Online application via respective State agriculture portals.",
        "official_website": "https://agrimachinery.nic.in/",
        "key_facts": [
            "Helps prevent stubble burning.",
            "Promotes Happy Seeder, Super Seeder, and zero-till machines."
        ],
        "keywords_hindi": ["पराली", "मशीन", "सब्सिडी"]
    },
    {
        "scheme_name": "State Disaster Response Fund (SDRF) - Agriculture Relief",
        "short_name": "SDRF Agri",
        "scheme_type": "income_support",
        "nodal_ministry": "Ministry of Home Affairs / State Governments",
        "launch_year": "2005",
        "objective": "To provide immediate relief to farmers for crop loss due to notified natural disasters, functioning as flood damaged crop compensation.",
        "target_beneficiaries": "Farmers who have suffered crop loss due to natural calamities.",
        "eligibility_rules": [
            "Crop loss must be 33% or more.",
            "Damage must be due to a notified disaster (flood, drought, hailstorm, pest attack, etc.)."
        ],
        "exclusions": [
            "Losses less than 33%.",
            "Damage due to negligence."
        ],
        "benefit_amount": "Varies by crop. e.g., Rs. 8,500/Ha for rainfed crops, Rs. 17,000/Ha for assured irrigated crops.",
        "premium_or_cost": "Nil.",
        "requirements": [
            "Aadhaar card",
            "Land records",
            "Bank passbook",
            "Photographic evidence of crop loss"
        ],
        "application_process": "Damage assessment is done jointly by Revenue and Agriculture departments. Claims are processed by District Administration.",
        "official_website": "https://ndmindia.mha.gov.in/",
        "key_facts": [
            "Provides ex-gratia assistance, not full compensation.",
            "Operates alongside PMFBY."
        ],
        "keywords_hindi": ["बाढ़", "मुआवजा", "राहत"]
    }
]

def run():
    print("Initializing SchemeDatabase...")
    db = SchemeDatabase(DB_PATH)
    
    # Run ID for this seeding task
    run_id = db.start_run("seed_custom_10", "https://manual-seed.local", 1, 1, 1, False)
    
    for scheme_data in CUSTOM_SCHEMES:
        print(f"Upserting scheme: {scheme_data['scheme_name']}")
        scheme_id = db.upsert_scheme(scheme_data, run_id=run_id, source_id=0)
        
        # Link source manually just in case
        if scheme_id:
            db.link_scheme_source(run_id, 0, scheme_id, "high", "manual-seed", {})
    
    db.finish_run(run_id, "completed", {"total": len(CUSTOM_SCHEMES)})
    
    print("Refreshing master dataset...")
    db.refresh_master_dataset()
    
    print("Refreshing curated schemes...")
    try:
        db.refresh_curated_schemes()
    except AttributeError:
        pass # Handle if refresh_curated_schemes doesn't exist
    
    # Enforce high confidence score and curated flag for these schemes
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    for scheme_data in CUSTOM_SCHEMES:
        name = scheme_data["scheme_name"]
        
        # Find scheme_id
        row = conn.execute("SELECT id FROM schemes WHERE scheme_name = ?", (name,)).fetchone()
        if row:
            sid = row["id"]
            # Update master_schemes
            conn.execute(
                "UPDATE master_schemes SET confidence_score = 99, curated_flag = 1 WHERE scheme_id = ?",
                (sid,)
            )
            # Update curated_schemes
            try:
                conn.execute(
                    "UPDATE curated_schemes SET confidence_score = 99, curated_flag = 1 WHERE scheme_id = ?",
                    (sid,)
                )
            except sqlite3.OperationalError:
                # Table might not exist yet if not fully populated
                pass
            print(f"Enforced 99% confidence for: {name}")
    
    conn.commit()
    conn.close()
    
    print("Done seeding database.")

if __name__ == "__main__":
    run()
