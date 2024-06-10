package edu.harvard.dbmi.avillach.avro.demo;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PFBDemo {

    public static final String PATIENT_ID = "Patient_ID";
    public static final String CONSENTS = "_consent";
    public static final String DEMOGRAPHICS_AGE = "tutorial_biolincc_framingham__AGE";
    public static final String STROKE = "tutorial_biolincc_framingham__STROKE";


    public static void main(String[] args) throws IOException {
        SchemaBuilder.FieldAssembler<Schema> entityFieldAssembler = SchemaBuilder.record("entity")
                .namespace("edu.harvard.dbmi")
                .fields();

        SchemaBuilder.FieldAssembler<Schema> nodeRecord = SchemaBuilder.record("nodes")
                .fields()
                .requiredString("name")
                .nullableString("ontology_reference", "null")
                .name("values").type(SchemaBuilder.map().values(SchemaBuilder.nullable().stringType())).noDefault();
        Schema nodeSchema = nodeRecord.endRecord();

        SchemaBuilder.FieldAssembler<Schema> metadataRecord = SchemaBuilder.record("metadata")
                .fields();
        metadataRecord.requiredString("misc");
        metadataRecord = metadataRecord.name("nodes").type(SchemaBuilder.array().items(nodeSchema)).noDefault();
        Schema metadataSchema = metadataRecord.endRecord();


        SchemaBuilder.FieldAssembler<Schema> patientRecords = SchemaBuilder.record("patientData")
                .fields();
        patientRecords.requiredInt(PATIENT_ID);
        patientRecords.requiredString(CONSENTS);
        patientRecords.requiredInt(DEMOGRAPHICS_AGE);
        patientRecords.requiredString(STROKE);
        Schema patientDataSchema = patientRecords.endRecord();

        Schema objectSchema = Schema.createUnion(metadataSchema, patientDataSchema);

        entityFieldAssembler = entityFieldAssembler.name("object").type(objectSchema).noDefault();
        entityFieldAssembler.nullableString("id", "null");
        entityFieldAssembler.requiredString("name");
        Schema entitySchema = entityFieldAssembler.endRecord();


        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(entitySchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        File file = new File("test-metadata2.avro");
        dataFileWriter.create(entitySchema, file);


        GenericRecord entityRecord = new GenericData.Record(entitySchema);


        List<GenericRecord> nodeList = new ArrayList<>();
        GenericRecord nodeData = new GenericData.Record(nodeSchema);
        nodeData.put("name", DEMOGRAPHICS_AGE);
        nodeData.put("ontology_reference", "Demographics");
        nodeData.put("values", Map.of("source", "caDSR",
                "cde_id", "2192217",
                "cde_version", "2.0"));
        nodeList.add(nodeData);

        nodeData = new GenericData.Record(nodeSchema);
        nodeData.put("name", STROKE);
        nodeData.put("ontology_reference", "Stroke");
        nodeData.put("values", Map.of("source", "caDSR",
                "cde_id", "4973892",
                "cde_version", "1.0"));
        nodeList.add(nodeData);


        GenericRecord metadata = new GenericData.Record(metadataSchema);
        metadata.put("misc", "some value");
        metadata.put("nodes", nodeList);


        entityRecord.put("object", metadata);
        entityRecord.put("name", "metadata");
        entityRecord.put("id", "null");

        dataFileWriter.append(entityRecord);


        GenericRecord patientData = new GenericData.Record(patientDataSchema);
        patientData.put(PATIENT_ID, 192035);
        patientData.put(CONSENTS, "tutorial-biolincc_framingham");
        patientData.put(DEMOGRAPHICS_AGE, 33);
        patientData.put(STROKE, "Event did not occur during followup");

        entityRecord = new GenericData.Record(entitySchema);
        entityRecord.put("object", patientData);
        entityRecord.put("name", "patientData");
        entityRecord.put("id", "192035");

        dataFileWriter.append(entityRecord);


        patientData = new GenericData.Record(patientDataSchema);
        patientData.put(PATIENT_ID, 192521);
        patientData.put(CONSENTS, "tutorial-biolincc_framingham");
        patientData.put(DEMOGRAPHICS_AGE, 33);
        patientData.put(STROKE, "Event did not occur during followup");

        entityRecord = new GenericData.Record(entitySchema);
        entityRecord.put("object", patientData);
        entityRecord.put("name", "patientData");
        entityRecord.put("id", "192521");

        dataFileWriter.append(entityRecord);


        patientData = new GenericData.Record(patientDataSchema);
        patientData.put(PATIENT_ID, 193172);
        patientData.put(CONSENTS, "tutorial-biolincc_framingham");
        patientData.put(DEMOGRAPHICS_AGE, 33);
        patientData.put(STROKE, "Event did not occur during followup");

        entityRecord = new GenericData.Record(entitySchema);
        entityRecord.put("object", patientData);
        entityRecord.put("name", "patientData");
        entityRecord.put("id", "193172");

        dataFileWriter.append(entityRecord);


        patientData = new GenericData.Record(patientDataSchema);
        patientData.put(PATIENT_ID, 194076);
        patientData.put(CONSENTS, "tutorial-biolincc_framingham");
        patientData.put(DEMOGRAPHICS_AGE, 33);
        patientData.put(STROKE, "Event did not occur during followup");

        entityRecord = new GenericData.Record(entitySchema);
        entityRecord.put("object", patientData);
        entityRecord.put("name", "patientData");
        entityRecord.put("id", "194076");

        dataFileWriter.append(entityRecord);


        patientData = new GenericData.Record(patientDataSchema);
        patientData.put(PATIENT_ID, 196077);
        patientData.put(CONSENTS, "tutorial-biolincc_framingham");
        patientData.put(DEMOGRAPHICS_AGE, 33);
        patientData.put(STROKE, "Event did not occur during followup");

        entityRecord = new GenericData.Record(entitySchema);
        entityRecord.put("object", patientData);
        entityRecord.put("name", "patientData");
        entityRecord.put("id", "196077");

        dataFileWriter.append(entityRecord);


        dataFileWriter.close();
    }
}
