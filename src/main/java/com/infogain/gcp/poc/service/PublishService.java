package com.infogain.gcp.poc.service;

import com.infogain.gcp.poc.component.MessageConverter;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.entity.PNROutBoxEntity;
import com.infogain.gcp.poc.model.PNRModel;
import com.infogain.gcp.poc.model.PublishMessageModel;
import com.infogain.gcp.poc.repository.PNROutBoxRepository;
import com.infogain.gcp.poc.repository.PNRRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Service
public class PublishService {

    @Value("${app.topic.name}")
    private String topicName;

    @Autowired
    private MessageConverter messageConverter;

    @Autowired
    private PubSubTemplate pubSubTemplate;

    @Autowired
    private PNRRepository pnrRepository;

    @Autowired
    private PNROutBoxRepository pnrOutBoxRepository;

    private String processMessage(PNREntity pnrEntity, Map<String, String> attributes) {
        log.info("Processing PNREntity={}", pnrEntity);

        if (StringUtils.isNotBlank(pnrEntity.getRemark())) {
            attributes.put("remark", pnrEntity.getRemark());
        }

        if (StringUtils.isNotBlank(pnrEntity.getMobileNumber())) {
            attributes.put("mobileNumber", pnrEntity.getMobileNumber());
        }

        String messageJson = messageConverter.convertToJsonString(pnrEntity);
        log.info("Processed PNREntity={}, messageJson={}", pnrEntity, messageJson);
        return messageJson;
    }

    private void publishMessage(String message, Map<String, String> attributes) {
        log.info("publishing message {} to topic {}", message, topicName);
        pubSubTemplate.publish(topicName, message, attributes);
        log.info("published message {} to topic {}", message, topicName);
    }

    private PNROutBoxEntity savePNROutBoxEntity(PNROutBoxEntity pnrOutBoxEntity) {
        return pnrOutBoxRepository.save(pnrOutBoxEntity);
    }

    private List<PNROutBoxEntity> saveOrUpdatePNROutBoxEntityList(List<PNROutBoxEntity> pnrOutBoxEntityList) {
        return IterableUtils.toList(pnrOutBoxRepository.saveAll(pnrOutBoxEntityList));
    }

    private List<PNROutBoxEntity> findPNROutBoxEntityListByPnrIdList(List<String> pnrIdList) {
        return IterableUtils.toList(pnrOutBoxRepository.findAllById(pnrIdList));
    }

    @Transactional
    public String publish(PNRModel pnrModel) {
        String pnrId = pnrModel.getPnrId();

        if (StringUtils.isEmpty(pnrId)) {
            log.info("pnrId is null or empty");
            return "pnrId is null or empty";
        }

//        log.info("find PNREntity by pnrId={}", pnrId);
//        Optional<PNREntity> pnrEntityOptional = pnrRepository.findPNREntityByPnrId(pnrId);

        PNREntity pnrEntity = pnrModel.buildEntity();
        Map<String, String> attributes = new HashMap<>();
        String messageJson = processMessage(pnrEntity, attributes);
        publishMessage(messageJson, attributes);

        // update PNROutBoxEntity.isProcessed to true, retryCount = retryCount + 1
        log.info("updating PNROutBoxEntity.isProcessed to true, retryCount = retryCount + 1");
        Optional<PNROutBoxEntity> pnrOutBoxEntityOptional = pnrOutBoxRepository.findById(pnrId);
        if (pnrOutBoxEntityOptional.isPresent()) {
            PNROutBoxEntity pnrOutBoxEntity = pnrOutBoxEntityOptional.get();
            pnrOutBoxEntity.setIsProcessed(true);
            pnrOutBoxEntity.setRetryCount(pnrOutBoxEntity.getRetryCount() + 1);
            savePNROutBoxEntity(pnrOutBoxEntity);
        }
        log.info("updated PNROutBoxEntity.isProcessed to true, retryCount = retryCount + 1");

        return "success";
    }

    public String publishFailedRecords() {
        List<PNREntity> pnrEntityList = ListUtils.emptyIfNull(pnrRepository.findPNREntityListByIsProcessedAndRetryCount(false, 5));

        if (CollectionUtils.isEmpty(pnrEntityList)) {
            log.info("No failed PNREntity records to process");
            return "success";
        }

        List<String> pnrIdList = new ArrayList<>();
        List<PublishMessageModel> publishMessageModelList = new ArrayList<>();


        pnrEntityList.forEach(pnrEntity -> {
            Map<String, String> attributes = new HashMap<>();
            String messageJson = processMessage(pnrEntity, attributes);
            PublishMessageModel publishMessageModel = PublishMessageModel.builder().message(messageJson).attributes(attributes).build();
            publishMessageModelList.add(publishMessageModel);
            pnrIdList.add(pnrEntity.getPnrId());
        });

        // TODO check to publish in batch rather than iterating
        publishMessageModelList.forEach(publishMessageModel -> {
            publishMessage(publishMessageModel.getMessage(), publishMessageModel.getAttributes());
        });

        // update PNROutBoxEntity.isProcessed to true, retryCount = retryCount + 1
        log.info("updating isProcessed to true, retryCount = retryCount + 1");
        List<PNROutBoxEntity> pnrOutBoxEntityList = findPNROutBoxEntityListByPnrIdList(pnrIdList);
        pnrOutBoxEntityList.forEach(pnrOutBoxEntity -> {
            pnrOutBoxEntity.setIsProcessed(true);
            pnrOutBoxEntity.setRetryCount(pnrOutBoxEntity.getRetryCount() + 1);
        });
        saveOrUpdatePNROutBoxEntityList(pnrOutBoxEntityList);
        log.info("updated isProcessed to true, retryCount = retryCount + 1");

        return "success";
    }

}