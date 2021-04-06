package com.infogain.gcp.poc.entity;

import org.springframework.cloud.gcp.data.spanner.core.mapping.Column;
import org.springframework.cloud.gcp.data.spanner.core.mapping.PrimaryKey;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "message_outbox_confirmation")
public class PNROutBoxConfirmationEntity {

	    @PrimaryKey
	    @Column(name = "id")
	    private String id;

	    @Column(name = "process_status")
	    private boolean processStatus;
    
}