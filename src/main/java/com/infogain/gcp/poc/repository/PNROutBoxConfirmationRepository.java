package com.infogain.gcp.poc.repository;

import org.springframework.cloud.gcp.data.spanner.repository.SpannerRepository;
import org.springframework.stereotype.Repository;

import com.infogain.gcp.poc.entity.PNROutBoxConfirmationEntity;

@Repository
public interface PNROutBoxConfirmationRepository extends SpannerRepository<PNROutBoxConfirmationEntity, String> {

	
	
}
