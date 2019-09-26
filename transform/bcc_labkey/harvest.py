# This script targets the client api version 0.4.0 and later
import labkey
import json
from pathlib import Path
import os.path

# saved labkey queryies
queries = [
  {
    'schema': 'ListManager',
    'query_names': ['ListManager']
  },
  {
    'schema': 'study',
    'query_names': [
        'admindemographics',
        'clinical_diagnosis',
        'clinical_narrative',
        'demographics',

        'biomarker_measurement_manually_entered',
        'biomarker_measurement_ohsu',
        'glycemic_lab_tests',
        'height_ohsu',
        'lesion_size',
        'weight_ohsu',

        'treatment_chemotherapy_regimen',
        'treatment_chemotherapy_manually_entered',
        'treatment_chemotherapy_ohsu',
        'Radiotherapy',

        'sample_genetrails_assay',
        'sample_genetrails_copy_number_variant',
        'sample_genetrails_sequence_variant',
        'sequencing_metadata_bcc',


        'WESResults',

        'biolibrary_specimen_distribution_annotation',
        'sample',
        'imaging_event',
        'image',
        "Biopsy",
        "BloodDraw",
        "CirculatingTumorAndHybridCells",
        "GeneTrailsCNV",
        "GeneTrailsResults",
        "GeneTrailsTrials",
        "GermlineVariants",
        "IHCImages",
        "IHCResults",
        "Imaging",
        "MutationSummary",
        "OmeroImages",
        "OncoLogTreatment",
        "OtherTreatments",
        "Outcomes",
        "Radiotherapy",
        "Samples",
        "Treatments",
        "WESResults",
        "biolibrary_specimen",
        "biolibrary_specimen_alias",
        "biolibrary_specimen_distribution_annotation",
        "biolibrary_specimen_generation",
        "biolibrary_specimen_treatment",
        "biomarker_measurement_manually_entered",
        "biomarker_measurement_ohsu",
        "clinical_diagnosis",
        "demographics",
        "height_manually_entered",
        "height_ohsu",
        "image",
        "image_participant_view",
        "imaging_event",
        "lesion_size",
        "mihc_assay",
        "mihc_results",
        "sample",
        "sample_genetrails_assay",
        "sample_genetrails_copy_number_variant",
        "sample_genetrails_sequence_variant",
        "sample_treatment_predicted_probability",
        "sample_treatment_probability_assay",
        "sequencing_metadata_bcc",
        "treatment_chemotherapy_manually_entered",
        "treatment_chemotherapy_ohsu",
        "treatment_chemotherapy_regimen",
        "weight_manually_entered",
        "weight_ohsu",
        "lesion_size",
        "Biopsy",
        "BloodDraw",
        "image_plot",
        "OtherTreatments",
        "CirculatingTumorAndHybridCells",
        "admindemographics"
    ]
  },
  {
    'schema': 'CompBioStudy',
    'query_names': [
        "histology_category_mapping",
        "oncolog_specimen",
        "v_specimen",
        "vbiolibraryspecimens",
        "vbiomarkermeasurement",
        "vcancerparticipantoverview",
        "vchemotherapy",
        "voncologdiagnosis",
        "voncologoutcomes",
        "voncologsurgery",
        "voncologtreatmentoverview",
        "vDemographicsForPlot",
        "vPlotDiagnosis",
        "vspecimenplot",
        "vResectionDate",
        "vPatientAliveAsOf",
        "vChemoCourseStart",
        "vChemotherapyCombo",
        "vRadiotherapyAnnotatedStartEnd",
        "vBiomarkerMeasurement",
        "vWeightMonthly",
        "oncolog_datafeed",
        "oncolog_primary_site",
        "cancer_status",
        "vascular_invasion"
    ]
  },
  {
    'schema': 'lists',
    'query_names': [
# <<<<<<< HEAD
#         "DisplayCategory",
#         "DisplaySubTab",
#         "DisplayTab",
#         "ImageType",
#         "RnaSeqMetaData",
#         "TumorBoard",
#         "UnitOfMeasure",
#         "allele",
#         "anatomical_site",
#         "anatomical_site_alt_name",
#         "assay",
#         "assay_categories",
#         "assay_version",
#         "biolibrary_diagnosis",
#         "biolibrary_diagnosis_alt_name",
#         "biolibrary_metastasis_site",
#         "biolibrary_recipient",
#         "biolibrary_sample_alias_type",
#         "biolibrary_sample_preparation_type",
#         "biolibrary_sample_storage_status",
#         "biolibrary_sample_treatment_type",
#         "biolibrary_sample_type",
#         "biolibrary_specimen_holder",
#         "biolibrary_specimen_people",
#         "biolibrary_specimen_project",
#         "body_composition",
#         "chromosome",
#         "clinical_event",
#         "clinical_significance",
#         "configuration_map",
#         "data_display_tabs",
#         "data_display_tabs_customizer",
#         "data_display_tabs_schema",
#         "data_owners",
#         "delivery_method",
#         "delivery_method_alt_name",
#         "delivery_route",
#         "diagnoses",
#         "diagnosis_grade",
#         "diagnosis_stage",
#         "diagnosis_type",
#         "disease_description",
#         "disposition_type",
#         "documentstore",
#         "gender",
#         "gene",
#         "genetrails_classification",
#         "genetrails_copy_number_result_type",
#         "genetrails_protein_variant_type",
#         "genetrails_result_significance",
#         "genetrails_result_type",
#         "genetrails_run_status",
#         "genetrails_transcript_priority",
#         "genetrails_variant_type",
#         "genome_build",
#         "glycemic_assay",
#         "glycemic_data_source",
#         "glycemic_unit_of_measure",
#         "image_category",
#         "image_type",
#         "lesion_procedure",
#         "lesion_site",
#         "lesion_type",
#         "malignancy",
#         "marker",
#         "method",
#         "mihc_allowed_cell_measure",
#         "mihc_cell_type",
#         "mihc_measure_type",
#         "participant_status",
#         "patientviewer_dashboard_componenttypes",
#         "protein_variant",
#         "reason_observation_not_available",
#         "sample_type",
#         "screening_outcome",
#         "sequencing_metadata_primary_contact",
#         "sequencing_metadata_product_notes_end_type",
#         "sequencing_metadata_product_notes_index",
#         "sequencing_metadata_product_notes_sequencer",
#         "sequencing_metadata_product_notes_sequencing_site",
#         "sequencing_metadata_protocol_hybrid_capture",
#         "sequencing_metadata_protocol_hybrid_capture_bait_set",
#         "sequencing_metadata_protocol_lib_construction",
#         "sequencing_metadata_protocol_lib_type",
#         "sequencing_metadata_sample_nucleic_acid_origin",
#         "sequencing_metadata_sample_source_location",
#         "sequencing_metadata_sample_tissue",
#         "sequencing_metadata_sample_type",
#         "sequencing_metadata_sample_type_preservation",
#         "status",
#         "study_name",
#         "tissue_site",
#         "treatment_agent",
#         "treatment_agent_alt_name",
#         "treatment_agent_generic_brand",
#         "treatment_combo",
#         "treatment_combo_agents",
#         "treatment_type",
#         "tumorboardpatient",
#         "unit_of_measure",
#         "unit_of_measure_alt_name",
#         "validation_severity",
# =======
        'allele',
        'anatomical_site',
        'anatomical_site_alt_name',
        'assay',
        'assay_categories',
        'assay_version',
        'biolibrary_diagnosis',
        'biolibrary_diagnosis_alt_name',
        'biolibrary_metastasis_site',
        'biolibrary_recipient',
        'biolibrary_sample_alias_type',
        'biolibrary_sample_preparation_type',
        'biolibrary_sample_storage_status',
        'biolibrary_sample_treatment_type',
        'biolibrary_sample_type',
        'biolibrary_specimen_holder',
        'biolibrary_specimen_people',
        'biolibrary_specimen_project',
        'body_composition',
        'chromosome',
        'clinical_event',
        'clinical_significance',
        'configuration_map',
        'data_display_tabs',
        'data_display_tabs_customizer',
        'data_display_tabs_schema',
        'data_owners',
        'delivery_method',
        'delivery_method_alt_name',
        'delivery_route',
        'diagnoses',
        'diagnosis_grade',
        'diagnosis_stage',
        'diagnosis_type',
        'disease_description',
        'DisplayCategory',
        'DisplaySubTab',
        'DisplayTab',
        'disposition_type',
        'documentstore',
        'gender',
        'gene',
        'genetrails_classification',
        'genetrails_copy_number_result_type',
        'genetrails_protein_variant_type',
        'genetrails_result_significance',
        'genetrails_result_type',
        'genetrails_run_status',
        'genetrails_transcript_priority',
        'genetrails_variant_type',
        'genome_build',
        'image_category',
        'image_type',
        'ImageType',
        'lesion_procedure',
        'lesion_site',
        'lesion_type',
        'malignancy',
        'marker',
        'method',
        'mihc_allowed_cell_measure',
        'mihc_cell_type',
        'mihc_measure_type',
        'participant_status',
        'patientviewer_dashboard_componenttypes',
        'protein_variant',
        'reason_observation_not_available',
        'RnaSeqMetaData',
        'sample_type',
        'screening_outcome',
        'sequencing_metadata_primary_contact',
        'sequencing_metadata_product_notes_end_type',
        'sequencing_metadata_product_notes_index',
        'sequencing_metadata_product_notes_sequencer',
        'sequencing_metadata_product_notes_sequencing_site',
        'sequencing_metadata_protocol_hybrid_capture',
        'sequencing_metadata_protocol_hybrid_capture_bait_set',
        'sequencing_metadata_protocol_lib_construction',
        'sequencing_metadata_protocol_lib_type',
        'sequencing_metadata_sample_nucleic_acid_origin',
        'sequencing_metadata_sample_source_location',
        'sequencing_metadata_sample_tissue',
        'sequencing_metadata_sample_type',
        'sequencing_metadata_sample_type_preservation',
        'status',
        'study_name',
        'tissue_site',
        'treatment_agent',
        'treatment_agent_alt_name',
        'treatment_agent_generic_brand',
        'treatment_combo',
        'treatment_combo_agents',
        'treatment_type',
        'TumorBoard',
        'tumorboardpatient',
        'unit_of_measure',
        'unit_of_measure_alt_name',
        'UnitOfMeasure',
        'validation_severity'        
# >>>>>>> 7ccf4f86a99b546f988c487eca8fdeddc4db259a
    ]
  },
]

# This script targets the client api version 0.4.0 and later
import labkey

server_context = labkey.utils.create_server_context('bcclabkey.ohsu.edu', 'Oregon Pancreatic Tumor Registry', use_ssl=True)


def check_credentials():
    """Ensures ~/.netrc file exists."""
    netrc = str(Path.home() / '.netrc')
    assert os.path.isfile(netrc), '.netrc should exist at {} see https://github.com/LabKey/labkey-api-python#set-up-a-netrc-file'.format(netrc)


def save_results(server_context, schema_name, query_name):
    """Saves results as json."""
    p = str( Path('.') / 'source' / 'bcc' / '{}.json'.format(query_name))
    if os.path.isfile(p):
        print('{} already exists'.format(p))
        return
    try:
        result = labkey.query.select_rows(server_context=server_context, schema_name=schema_name, query_name=query_name)
<<<<<<< HEAD
        if result['rowCount'] == 0:
            print('{}.{} has no data'.format(schema_name, query_name))
            return

        with open(p, 'w') as out:
            for r in result['rows']:
                out.write(json.dumps(r, separators=(',', ':')))
                out.write('\n')
        print(query_name, result['rowCount'])
    except Exception as e:
        print(query_name, e)
=======
    except Exception as e:
        print(e)
        return
    if result['rowCount'] == 0:
        print('{}.{} has no data'.format(schema_name, query_name))
        return
>>>>>>> 7ccf4f86a99b546f988c487eca8fdeddc4db259a



check_credentials()
# save all results
ctx = labkey.utils.create_server_context('bcclabkey.ohsu.edu', 'Oregon Pancreatic Tumor Registry', use_ssl=True)
for q in queries:
  s = q['schema']
  for n in q['query_names']:
      save_results(ctx, s, n)
