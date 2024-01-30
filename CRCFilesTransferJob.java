/*
 *
 * Copyright (c) 2013 Jocata, LLC
 * 445 Park Avenue, 9th Floor,  New York,  NY 10022,  U.S.A
 * All rights reserved.
 * 
 * This software is the confidential and proprietary information of Jocata, LLC
 * ("Confidential Information"). You shall not disclose such Confidential Information
 * and shall use it only in accordance with the terms of the license agreement you
 * entered into with Jocata.
 */
package com.star.jobs.rules;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.hibernate.query.Query;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aml.biz.service.AmlServiceManager;
import com.aml.constants.AmlPropertyConstants;
import com.aml.db.entity.GridJobTriggers;
import com.aml.hibernate.HibernateSessionManager;
import com.aml.utils.UserUtil;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.opencsv.CSVWriter;
import com.star.jobs.service.StarJobsServiceManager;
import com.star.constants.StarConstants;
import com.star.constants.StarJobConstants;
import com.star.db.entity.AiProcessSummary;
import com.star.db.entity.CRCJobLog;
import com.star.exception.StarSystemException;
import com.star.jcms.constants.JcmsConstants;
import com.star.jobs.StarBaseJobBean;
import com.star.util.AESEncryptDecryptUtil;
import com.star.util.DateTimeUtils;

/**
 * @author anil.muddineni
 *
 */
public class CRCFilesTransferJob extends StarBaseJobBean implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(CRCFilesTransferJob.class);
	private AmlServiceManager amlService;
	private StarJobsServiceManager starService;
	Integer limit = null;




	/* (non-Javadoc)
	 * @see org.springframework.scheduling.quartz.QuartzJobBean#executeInternal(org.quartz.JobExecutionContext)
	 */
	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		logger.info("CRC File Transfer Job Started");	
		GridJobTriggers gridJobTriggersObj = null;
		Long endTime = null;
		boolean flag = true;
		Long startTime = null;

		try {
			starService = getStarService(context);
			amlService = getAmlServiceManager(context);
			startTime = System.currentTimeMillis();
			limit = (Integer) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_FILE_WRTIE_BATCH_LIMIT, Integer.class);	
			
			String SFTP_CLIENT_UPLOAD_DIR = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_CLIENT_UPLOAD_DIR, String.class);
			
			String AUTH_KEY_PATH = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_AUTH_KEY_PATH, String.class);	
			
//			String ENCRYPT_PUBLIC_KEY_FILE_NAME = starProperties.getSftpEncryptionPublicKeyFileName();			
			Long jobLastRunDate = DateTimeUtils.getCurrentDateAndTimeinMillis();
			gridJobTriggersObj = this.getStarService(context).getJobsCommonService().getCronJobRunDetails(StarJobConstants.JOB_CRC_FILE_TRANSFER);
			if (gridJobTriggersObj == null) {
				gridJobTriggersObj = new GridJobTriggers();
				gridJobTriggersObj.setJobName(StarJobConstants.JOB_CRC_FILE_TRANSFER);
				gridJobTriggersObj.setJobDesc(StarJobConstants.JOB_CRC_FILE_TRANSFER_DESC);
			} else {
				jobLastRunDate = gridJobTriggersObj.getLastRunDate();
			}
			if(null != SFTP_CLIENT_UPLOAD_DIR && null != AUTH_KEY_PATH){
				File clientUploadDir = new File(SFTP_CLIENT_UPLOAD_DIR);
				File authKeyDir = new File(AUTH_KEY_PATH);				
//				File authPublicKey = new File(AUTH_KEY_PATH+ENCRYPT_PUBLIC_KEY_FILE_NAME);
				if(!clientUploadDir.exists()){
					flag = false;
					throw new StarSystemException("SFTP Client "+SFTP_CLIENT_UPLOAD_DIR+" directory doesn't exist");
				}
				if(!authKeyDir.exists()){
					flag = false;
					throw new StarSystemException("SFTP Client "+AUTH_KEY_PATH+" directory doesn't exist");			
				}
				/*if(!authPublicKey.exists()){
					flag = false;
					throw new StarSystemException("SFTP Client "+AUTH_KEY_PATH+ENCRYPT_PUBLIC_KEY_FILE_NAME+" doesn't exist");
				}*/
				String targetTableName = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_TARGET_TABLE_NAME, String.class);
				String csvFileFromat = StarJobConstants.CRC_CSV_FILE_FORMAT; 
				String fileName = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_FILE_NAME, String.class);
				String sourceSystem = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_SOURCESYSTEM, String.class);
				String ecbfFileHeaders = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_FILE_HEADERS, String.class);
				String selectColumns = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_FILE_SELECT_COLUMNS, String.class);
				String ecbfFileDateformat = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_FILENAME_DATE_FORMAT, String.class);
				String SFTP_CLIENT_UPLOAD_DIR_ECBF = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_CLIENT_UPLOAD_DIR_ECBF, String.class);
				String SFTP_SERVER_UPLOAD_DIR_ECBF = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_SERVER_UPLOAD_DIR_ECBF, String.class);
				Boolean processECBF = (Boolean) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_ECBF_ISENABLED, Boolean.class);
				try {
					if (processECBF && !StringUtils.isEmpty(fileName) 
							&& !StringUtils.isEmpty(sourceSystem)
							&& !StringUtils.isEmpty(ecbfFileHeaders) 
							&& !StringUtils.isEmpty(selectColumns)
							&& !StringUtils.isEmpty(ecbfFileDateformat)
							&& !StringUtils.isEmpty(SFTP_CLIENT_UPLOAD_DIR_ECBF)
							&& !StringUtils.isEmpty(SFTP_SERVER_UPLOAD_DIR_ECBF)
							&& !StringUtils.isEmpty(targetTableName)) {
						logger.info(" ECBF Source System Files processing... ");
						CRCJobLog ecbfCRCJobLog = new CRCJobLog();
						ecbfCRCJobLog.setJobRunDate(startTime);
						ecbfCRCJobLog.setJobStartDate(startTime);
						ecbfCRCJobLog.setSourceSystem(sourceSystem);
						String selectQuery = StarJobConstants.PROP_CONST_CRC_ECBF_SELECT_QUERY + " WHERE FI_PRINCIPAL_ID LIKE '"+sourceSystem+"%' ";
						flag = this.prepareCRCDataFilesandUploadToSFTP(targetTableName, csvFileFromat, fileName, sourceSystem,
								ecbfFileHeaders, selectColumns, ecbfFileDateformat, SFTP_CLIENT_UPLOAD_DIR_ECBF, SFTP_SERVER_UPLOAD_DIR_ECBF, ecbfCRCJobLog, selectQuery);
						if (flag) {
							ecbfCRCJobLog.setFileUploadStatus((byte)1);
							logger.info(" ECBF Source System Files processing completed. ");
						} else {
							ecbfCRCJobLog.setFileUploadStatus((byte)0);
							logger.info(" ECBF Source System Files processing failed. ");
						}
						ecbfCRCJobLog.setJobEndDate(System.currentTimeMillis());
						starService.getJobsCommonService().saveObject(ecbfCRCJobLog);
					} else {
						logger.info(" ECBF Source System product config values are missing or disabled, please check ");
					}
					
				} catch (Exception e) {
					logger.error("Error while processing ECBF files in CRC file transfer job --->", e);
					flag = false;
				}
				
				String VPfileName = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_FILE_NAME, String.class);
				String vpSourceSystem = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_SOURCESYSTEM, String.class);
				String visionPlusHeaders = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_FILE_HEADERS, String.class);
				String vpSelectColumns = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_FILE_SELECT_COLUMNS, String.class);
				String vpFileDateFormat = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_FILENAME_DATE_FORMAT, String.class);
				String SFTP_CLIENT_UPLOAD_DIR_VISION = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_CLIENT_UPLOAD_DIR_VISIONPLUS, String.class);
				String SFTP_SERVER_UPLOAD_DIR_VP = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_SERVER_UPLOAD_DIR_VP, String.class);
				Boolean processVP = (Boolean) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_VP_ISENABLED, Boolean.class);
				try {
					if (processVP && !StringUtils.isEmpty(VPfileName) 
							&& !StringUtils.isEmpty(vpSourceSystem)
							&& !StringUtils.isEmpty(visionPlusHeaders) 
							&& !StringUtils.isEmpty(vpSelectColumns)
							&& !StringUtils.isEmpty(vpFileDateFormat)
							&& !StringUtils.isEmpty(SFTP_CLIENT_UPLOAD_DIR_VISION)
							&& !StringUtils.isEmpty(SFTP_SERVER_UPLOAD_DIR_VP)
							&& !StringUtils.isEmpty(targetTableName)) {
						logger.info(" Vision Plus Source System Files processing... ");
						CRCJobLog vpCRCJobLog = new CRCJobLog();
						vpCRCJobLog.setJobRunDate(startTime);
						vpCRCJobLog.setJobStartDate(startTime);
						vpCRCJobLog.setSourceSystem(vpSourceSystem);
						String selectQuery = this.prepareSelectQuery(vpSelectColumns, targetTableName, vpSourceSystem);
						flag = this.prepareCRCDataFilesandUploadToSFTP(targetTableName, csvFileFromat, VPfileName, vpSourceSystem,
								visionPlusHeaders, vpSelectColumns, vpFileDateFormat, SFTP_CLIENT_UPLOAD_DIR_VISION, SFTP_SERVER_UPLOAD_DIR_VP, vpCRCJobLog, selectQuery);
						if (flag) {
							vpCRCJobLog.setFileUploadStatus((byte)1);
							logger.info(" Vision Plus Source System Files processing completed. ");
						} else {
							vpCRCJobLog.setFileUploadStatus((byte)0);
							logger.info(" Vision Plus Source System Files processing failed. ");
						}
						vpCRCJobLog.setJobEndDate(System.currentTimeMillis());
						starService.getJobsCommonService().saveObject(vpCRCJobLog);
					} else {
						logger.info(" Vision Plus Source System product config values are missing or disabled, please check ");
					}
				} catch (Exception e) {
					logger.error("Error while processing Vision Plus files in CRC file transfer job --->", e);
					flag = false;
				}
			
				String txtFileFromat = StarJobConstants.CRC_TXT_FILE_FORMAT;
				String fisFileName = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_FIS_FILE_NAME, String.class);
				String fisSourceSystem = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_FIS_SOURCESYSTEM, String.class);
				String fisHeaders = null; //client wants no headers to fis
				String fisSelectColumns = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_FIS_SELECT_COLUMNS, String.class);
				String fisFileDateFormat = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_FIS_FILENAME_DATE_FORMAT, String.class);
				String SFTP_CLIENT_UPLOAD_DIR_FIS = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_CLIENT_UPLOAD_DIR_FIS, String.class);
				String SFTP_SERVER_UPLOAD_DIR_FIS = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_SFTP_SERVER_UPLOAD_DIR_FIS, String.class);
				Boolean processFIS = (Boolean) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
						amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
						StarJobConstants.PROP_CONST_CRC_FIS_ISENABLED, Boolean.class);
				try {
					if (processFIS && !StringUtils.isEmpty(fisFileName) 
							&& !StringUtils.isEmpty(fisSourceSystem)
							&& !StringUtils.isEmpty(fisSelectColumns) 
							&& !StringUtils.isEmpty(fisFileDateFormat)
							&& !StringUtils.isEmpty(SFTP_CLIENT_UPLOAD_DIR_FIS)
							&& !StringUtils.isEmpty(SFTP_SERVER_UPLOAD_DIR_FIS)
							&& !StringUtils.isEmpty(targetTableName)) {
						logger.info(" FIS Source System Files processing... ");
						
						CRCJobLog fisCRCJobLog = new CRCJobLog();
						fisCRCJobLog.setJobRunDate(startTime);
						fisCRCJobLog.setJobStartDate(startTime);
						fisCRCJobLog.setSourceSystem(fisSourceSystem);
						String selectQuery = this.prepareSelectQuery(fisSelectColumns, targetTableName, fisSourceSystem);
						flag = this.prepareCRCDataFilesandUploadToSFTP(targetTableName, txtFileFromat, fisFileName, fisSourceSystem,
								fisHeaders, fisSelectColumns, fisFileDateFormat, SFTP_CLIENT_UPLOAD_DIR_FIS, SFTP_SERVER_UPLOAD_DIR_FIS, fisCRCJobLog, selectQuery);
						if (flag) {
							fisCRCJobLog.setFileUploadStatus((byte)1);
							logger.info(" FIS Source System Files processing completed. ");
						} else {
							fisCRCJobLog.setFileUploadStatus((byte)0);
							logger.info(" FIS Source System Files processing failed. ");
						}
						fisCRCJobLog.setJobEndDate(System.currentTimeMillis());
						starService.getJobsCommonService().saveObject(fisCRCJobLog);
					} else {
						logger.info(" FIS Source System product config values are missing or disabled, please check ");
					}
				} catch (Exception e) {
					logger.error("Error while processing FIS files in CRC file transfer job --->", e);
					flag = false;
				}
			}
		}catch (StarSystemException e) {
			logger.error("Error in CRC file transfer job --->", e);
			flag = false;
		} catch (Exception e) {
			logger.error("Error in CRC file transfer job --->", e);
			flag = false;
		}finally {
			endTime = System.currentTimeMillis();
			gridJobTriggersObj.setLastRunDate(endTime);
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				logger.info("Error --> ", e);	
			}
			System.gc();
		}
		logger.info("CRC File TransferJob ended at " + new Date(endTime));
		if (startTime != null)
			logger.info("Execution time for the job ---> CRC File TransferJob is " + (endTime - startTime) / 1000 + " seconds");

		// call post job actions before leaving the job handler, Hibernate session will be closed inside
		handlePostCronJobActions(gridJobTriggersObj, flag ? (byte)1 : (byte)0);
	}
	
	public boolean prepareCRCDataFilesandUploadToSFTP(String targetTableName, String fileFromat, String fileName, String sourceSystem, String headersStr, 
			String selectColumns, String FileDateformat, String SFTP_CLIENT_UPLOAD_DIR, String SFTP_SERVER_UPLOAD_DIR, CRCJobLog crcJobLog, String selectQuery) { 
		boolean success = false;
		Long currTimeStamp = System.currentTimeMillis();
		try {
			String[] fileHeaders = !StringUtils.isEmpty(headersStr) ? headersStr.split(",") : null;
			//As we are checking count for source system level, using count(1) on target table and not using select data query
			Long dataCount = starService.getJobsCommonService().getCountFromFinalRiskBySourceSystem(targetTableName, sourceSystem);
			if(dataCount > 0){
				int numberOfDBRequests = 1;
				if(dataCount > limit){	
					numberOfDBRequests = Double.valueOf(Math.floor(dataCount.intValue()/limit)).intValue() + 1;
				}
				int index = 0;
				int start = 0;	
				Iterator itr = null;
				List<Object[]> resultList = null;
				List<String[]> csvData = null;
				String[] stringArray = null;
				Object[] objectArray = null;
				if(fileFromat.equalsIgnoreCase("csv")) {
				//Processing csv files	
				fileName = fileName+"_"+getCurrentTimeInRequiredFormat(FileDateformat) + ".csv";
				File file = new File(SFTP_CLIENT_UPLOAD_DIR+fileName);
				file.deleteOnExit();
				logger.info(fileName + " file writing has been started at -----> "+new Date(currTimeStamp));	
				CSVWriter csvWriter = new CSVWriter(new FileWriter(SFTP_CLIENT_UPLOAD_DIR+fileName,true),'|',CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.NO_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END);
				Long totalRecords = 0l;
				while(index < numberOfDBRequests){
					start = index * limit;
					resultList = starService.getJobsCommonService().getCRCDataFromRiskFinalTable(selectQuery, start, limit);
					if(null != resultList && resultList.size() > 0){
						itr = resultList.iterator();
						csvData = new ArrayList<String[]>();
						if(index == 0){
							csvData.add(fileHeaders);
						}
						while(itr.hasNext()){
							objectArray = (Object[]) itr.next();
							stringArray = new String[objectArray.length];
							for(int i=0;i<objectArray.length;i++){
								if(null != objectArray[i] && !objectArray[i].equals("null") && !objectArray[i].equals("")){
									stringArray[i] = objectArray[i].toString();
								}
								else{
									stringArray[i] = "-";
								}
							}
							csvData.add(stringArray);
							totalRecords++;
						}
						csvWriter.writeAll(csvData);
					}
					index++;
				}
				csvWriter.close();
				Long csvWriteEndTime = System.currentTimeMillis();						
				logger.info(fileName +" file writing has been ended at : "+new Date(csvWriteEndTime));
				logger.info(" Total records written to "+ fileName +" file are : "+(totalRecords-1));
				logger.info(" Total time taken for CSV file writing is : "+((csvWriteEndTime-currTimeStamp)/1000)+" seconds");
				crcJobLog.setRecordsCount(totalRecords-1);
				}else {
					//Processing FIS .txt format files 
					String fisRiskCodes = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
							amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
							StarJobConstants.PROP_CONST_CRC_FIS_RISK_CODES, String.class);
					Map<Object, Object> riskCodesHM = new HashMap<>();
					if(!StringUtils.isEmpty(fisRiskCodes)) {  //0,LOW|1,HIGH|5,MEDIUM
						riskCodesHM =  Arrays.stream(fisRiskCodes.split("\\|"))
				                .map(s -> s.split(","))
				                .collect(Collectors.toMap(a -> a[0], a -> a[1]));
					}
					fileName = fileName+"_"+getCurrentTimeInRequiredFormat(FileDateformat) + ".txt";
					logger.info(fileName + " file writing has been started at -----> "+new Date(currTimeStamp));	
					File file = new File(SFTP_CLIENT_UPLOAD_DIR+fileName);
					file.deleteOnExit();
					FileOutputStream fileOutputStream = new FileOutputStream(file);
					OutputStreamWriter osw = new OutputStreamWriter(fileOutputStream);
					Writer textWriter = new BufferedWriter(osw);
					Long totalRecords = 0l;
					while(index < numberOfDBRequests){
							start = index * limit;
							resultList = starService.getJobsCommonService().getCRCDataFromRiskFinalTable(selectQuery, start, limit);
							if(null != resultList && resultList.size() > 0){
								itr = resultList.iterator();
								csvData = new ArrayList<String[]>();
								while(itr.hasNext()){
									objectArray = (Object[]) itr.next();
									StringBuilder sd = new StringBuilder();
									if(null != objectArray[0] && "null" != objectArray[0]){
											sd.append(riskCodesHM.get(objectArray[0])).append("#");
									}else {
										sd.append("-").append("#");
									}
									if(null != objectArray[1] && "null" != objectArray[1]){
										sd.append(objectArray[1].toString()).append("#");
									}else {
										sd.append("-").append("#");
									}
									textWriter.write(sd.toString());
									totalRecords++;
								}
							}
							index++;
						}
						textWriter.close();
						Long csvWriteEndTime = System.currentTimeMillis();						
						logger.info(fileName+" file writing has been ended at : "+new Date(csvWriteEndTime));
						logger.info(" Total records written to "+ fileName +" file are : "+(totalRecords-1));
						logger.info(" Total time taken for .txt file writing is : "+((csvWriteEndTime-currTimeStamp)/1000)+" seconds");
						crcJobLog.setRecordsCount(totalRecords-1);
				}
				crcJobLog.setFileName(fileName);
				crcJobLog.setFileCreationStatus((byte) 1);
				success = this.uploadFileThroughSFTP(fileName, SFTP_CLIENT_UPLOAD_DIR, SFTP_SERVER_UPLOAD_DIR, crcJobLog);
			}
		}catch(Exception e) {
			success = false;
			crcJobLog.setFileCreationStatus((byte) 0);
			logger.error("Error in CRC file transfer job while processing csv file --->", e);
		}
		return success;
	}
	
	public boolean uploadFileThroughSFTP(String uploadFileName, String SFTP_CLIENT_UPLOAD_DIR, String SFTP_SERVER_UPLOAD_DIR, CRCJobLog crcJobLog) {
		logger.info(" Uploading CRC file "+ uploadFileName +" through SFTP has been started... ");	
		boolean success = false;
		JSch jsch = new JSch();
		Session session = null;
        ChannelSftp channelSftp = null;
		File inputFile = null;


		try{
			String SFTP_HOST = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_HOST, String.class);
			Integer SFTP_PORT = (Integer) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_PORT, Integer.class);	
			String SFTP_USER = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_USERNAME, String.class);
			String SFTP_PASS = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_PASSWORD, String.class);
			String AUTH_KEY_PATH = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_AUTH_KEY_PATH, String.class);		
			String AUTH_KEY_FILE_NAME = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_AUTH_KEY_FILE_NAME, String.class);	
			String SFTP_CIPHER = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_CIPHER, String.class);
			String privateKeyToConnectSFTP = (String) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_PRIVATE_KEY_PATH, String.class);
			Boolean isPrivateKeyReqForSFTPAuth = (Boolean) amlService.getProductConfigurationService().getProductConfigValueByProductIdAndPropertyKey(
					amlService.getProductConfigurationService().getProductByProductName(AmlPropertyConstants.CONST_PRODUCT_NAME_STAR).getProductId(),
					StarJobConstants.PROP_CONST_CRC_SFTP_PRIVATE_KEY_REQUIRED, Boolean.class); 
			
			if(!StringUtils.isEmpty(SFTP_HOST) && null != SFTP_PORT
			&& !StringUtils.isEmpty(SFTP_USER) && !StringUtils.isEmpty(SFTP_SERVER_UPLOAD_DIR)
			&& !StringUtils.isEmpty(SFTP_CLIENT_UPLOAD_DIR) && !StringUtils.isEmpty(AUTH_KEY_PATH)
			&& !StringUtils.isEmpty(AUTH_KEY_FILE_NAME))
			{
				File keyFile = new File(AUTH_KEY_PATH + AUTH_KEY_FILE_NAME);
				if(!keyFile.exists()){
					success = false;
					throw new StarSystemException("Key file for authentication is not found");
				}
				//jsch.addIdentity(AUTH_KEY_PATH + AUTH_KEY_FILE_NAME);
				if(isPrivateKeyReqForSFTPAuth && privateKeyToConnectSFTP != null)
				{
					logger.info(" SFTP Authentication using private key ");
					jsch.addIdentity(privateKeyToConnectSFTP);
				}
				session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
				session.setPassword(UserUtil.decode(SFTP_PASS));
				session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
				if(null != SFTP_CIPHER && SFTP_CIPHER.trim().length() > 0){
					session.setConfig("cipher.s2c", SFTP_CIPHER);
					session.setConfig("cipher.c2s", SFTP_CIPHER);
					session.setConfig("CheckCiphers", SFTP_CIPHER);
				}
				session.setConfig("StrictHostKeyChecking", "no");				
				session.connect();
				logger.info("SFTP Session is connected..");
				channelSftp = (ChannelSftp) session.openChannel("sftp");
				channelSftp.connect();
				logger.info("SFTP Channel is opened and connected....");	

				boolean serverDirExists = checkServerDirExistance(channelSftp, SFTP_SERVER_UPLOAD_DIR);
				if(serverDirExists){
					File clientDir = new File(SFTP_CLIENT_UPLOAD_DIR);
					if(clientDir.exists() && clientDir.isDirectory()){
						channelSftp.cd(SFTP_SERVER_UPLOAD_DIR);
						String fileName = SFTP_CLIENT_UPLOAD_DIR + uploadFileName;
						inputFile = new File(fileName);
						if(!inputFile.exists()){
							success = false;
							throw new StarSystemException("File "+fileName+" doesn't exist");
						}	
						channelSftp.put(new FileInputStream(inputFile), inputFile.getName());
						logger.info("File "+inputFile.getName()+" has been uploaded successfully to SFTP host....");
						success = true;
					}
					else{
						success = false;
						throw new StarSystemException("SFTP Client "+SFTP_CLIENT_UPLOAD_DIR+" directory doesn't exist");
					}
				}
				else{
					success = false;
					throw new StarSystemException("SFTP Server "+SFTP_SERVER_UPLOAD_DIR+" directory doesn't exist");
				}
			}
			else{
				success = false;
				throw new StarSystemException("Empty values for SFTP properties in star proprties file");
			}
			
		} catch(StarSystemException e){
			success = false;			
			logger.error("Error occured in method uploadFileThroughSFTP in CRC File Transfer job -->", e);
		} catch (JSchException e) {
			success = false;			
			logger.error("Error occured while opening an SFTP session or channel in CRC File Transfer job -->", e);
		} catch (IOException e) {
			success = false;			
			logger.error("Error occured while accessing the file in CRC File Transfer job -->", e);
		} catch (Exception e) {
			success = false;			
			logger.error("Error occured while uploading the file through SFTP in CRC File Transfer job -->", e);
		} finally{
			if(null != channelSftp){
				channelSftp.disconnect();
				logger.info("SFTP Channel is disconnected...");
			}
			if(null != session){
				session.disconnect();
				logger.info("SFTP Session is disconnected...");
			}
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				logger.info("Error --> ", e);	
			}
			System.gc();
		}
		logger.info("Uploading file through SFTP has been ended....");	
		return success;
	}
	
	public boolean checkServerDirExistance(ChannelSftp channelSftp, String serverPath){
		boolean dirExists = true;
		try{
			channelSftp.ls(serverPath);
		}catch(Exception e){
			dirExists = false;
		}
		return dirExists;
	}
	
	public static String getCurrentTimeInRequiredFormat(String dateFormat) {
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat sdf;
        sdf = new SimpleDateFormat(dateFormat);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }
	
	public String prepareSelectQuery(String selectColumns, String targetTableName, String sourceSystem) {
		StringBuilder sd = new StringBuilder();
		sd.append(" select /*+ PARALLEL */ " +selectColumns);
		sd.append(" FROM "+targetTableName);
		sd.append(" WHERE FI_PRINCIPAL_ID LIKE '"+sourceSystem+"%'");
		return sd.toString();
	}

}
