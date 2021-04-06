package com.infogain.gcp.poc.repository;

import com.infogain.gcp.poc.entity.PNROutBoxConfirmationEntity;
import com.infogain.gcp.poc.entity.PNROutBoxEntity;
import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;
import org.springframework.cloud.gcp.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PNROutBoxRepository extends SpannerRepository<PNROutBoxEntity, String> {

    List<PNROutBoxEntity> findByPnrId(String pnrId);
    

    @Query(value = "select pob.payload as payload,pob.id as id from pnr_out_box pob where   \r\n" + 
    		"    pob.id not IN (select id from message_outbox_confirmation) UNION ALL  \r\n" + 
    		"    select pob.payload as payload,pob.id as id from pnr_out_box pob where   \r\n" + 
    		"    pob.id  IN (select id from message_outbox_confirmation WHERE  process_status = @processStatus)")
    List<PNROutBoxEntity> findUnProcessedPayload(@Param("processStatus")Boolean processStatus);

}