/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo;

import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.runtime.ObjectEntityMapper;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.impl.ObjectEntityMapperImpl;

public class ObjectEntityMapperImplTest {
	
	ObjectEntityMapper objectEntityMapper;
	
	@Mock
	RuntimeInfoDTO mockRuntimeInfoDTO;
	
	@Mock
	RuntimeInfo mockRuntimeInfo;
	
	@Mock
	RuntimePropertyDTO mockRuntimePropertyDTO;
	
	@Mock
	Set<RuntimePropertyDTO> mockRuntimePropertyDTOSet;
	
	@Mock
	List<RuntimeInfoDTO> mockRuntimeInfoDTOList;
	
	
	
	@Mock
	Iterator<RuntimePropertyDTO> iterator;
	@BeforeTest
	public void init(){
		initMocks(this);
		objectEntityMapper = new ObjectEntityMapperImpl();
		//ReflectionTestUtils.setField(objectEntityMapper, "properties", mockMap);
		Mockito.when(mockRuntimeInfoDTO.getRuntimeId()).thenReturn(1);
		Mockito.when(mockRuntimeInfoDTO.getAdaptorName()).thenReturn("testAdaptorName");
		Mockito.when(mockRuntimeInfoDTO.getEntityName()).thenReturn("testEntityName");
		Mockito.when(mockRuntimeInfoDTO.getInputDescriptor()).thenReturn("testInputDescriptor");
		Mockito.when(mockRuntimeInfoDTO.getStatus()).thenReturn(Status.ROLLED_BACK);
		
		
		Mockito.when(mockRuntimeInfoDTO.getRuntimeProperties()).thenReturn(mockRuntimePropertyDTOSet);
		Mockito.when(mockRuntimePropertyDTOSet.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true,false);
		
		Mockito.when(iterator.next()).thenReturn(mockRuntimePropertyDTO);
		Mockito.when(mockRuntimePropertyDTOSet.size()).thenReturn(1);
	    Mockito.when(mockRuntimePropertyDTO.getKey()).thenReturn("testKey");
	    Mockito.when(mockRuntimePropertyDTO.getValue()).thenReturn("testValue");
	}
	
	@Test
	public void testMapObjectList() {
		
		//Mockito.when(mockRuntimeInfoDTO.getRuntimeProperties()).thenReturn(mockRuntimePropertyDTOSet);
		Mockito.when(mockRuntimeInfoDTOList.size()).thenReturn(1);
		Iterator<RuntimeInfoDTO> iteratorRuntimeInfoDTO = Mockito.mock(Iterator.class);
		Mockito.when(mockRuntimeInfoDTOList.iterator()).thenReturn(iteratorRuntimeInfoDTO);
		Mockito.when(iteratorRuntimeInfoDTO.hasNext()).thenReturn(true,false);
		Mockito.when(iteratorRuntimeInfoDTO.next()).thenReturn(mockRuntimeInfoDTO);
		//Mockito.when(mockRuntimePropertyDTO.getKey()).thenReturn("testKey1");
	    //Mockito.when(mockRuntimePropertyDTO.getValue()).thenReturn("testValue1");
		objectEntityMapper.mapObjectList(mockRuntimeInfoDTOList);
		
		
	}
	
	@Test
	public void testNullMapObjectList() {
		Mockito.when(mockRuntimeInfoDTOList.size()).thenReturn(0);
		objectEntityMapper.mapObjectList(mockRuntimeInfoDTOList);
		
	}
	
	@Test
	public void testMapObject(){
		
		
		
		RuntimeInfo runtimeInfo = objectEntityMapper.mapObject(mockRuntimeInfoDTO);
		Assert.assertNotNull(runtimeInfo);
	}
	
	@Test
	public void testNullMapObject() {
		Assert.assertNull(objectEntityMapper.mapObject(null));
	}
	
	
	@Test
	public void testMapEntityObject() {
		Mockito.when(mockRuntimeInfo.getAdaptorName()).thenReturn("testAdaptorName");
		Mockito.when(mockRuntimeInfo.getEntityName()).thenReturn("testEntityName");
		Mockito.when(mockRuntimeInfo.getInputDescriptor()).thenReturn("testInputDescriptor");
		Mockito.when(mockRuntimeInfo.getStatus()).thenReturn(Status.STARTED);
		
		Map<String,String> mockMap = Mockito.mock(Map.class);
		Set<Entry<String,String>> mockSet = Mockito.mock(Set.class);
		Iterator<Entry<String,String>> mockIteratorEntry = Mockito.mock(Iterator.class);
		Entry<String,String> mockEntry = Mockito.mock(Entry.class);
		Mockito.when(mockRuntimeInfo.getProperties()).thenReturn(mockMap);
		Mockito.when(mockMap.entrySet()).thenReturn(mockSet);
		Mockito.when(mockSet.iterator()).thenReturn(mockIteratorEntry);
		Mockito.when(mockIteratorEntry.hasNext()).thenReturn(true,false);
		Mockito.when(mockIteratorEntry.next()).thenReturn(mockEntry);
		
		Mockito.when(mockEntry.getKey()).thenReturn("testKey");
		Mockito.when(mockEntry.getValue()).thenReturn("testValue");
		
		
		//ReflectionTestUtils.setField(objectEntityMapper, "runtimePropertyDTOSet", mockRuntimePropertyDTOSet);
		Mockito.when(mockRuntimePropertyDTOSet.size()).thenReturn(2);
		Assert.assertNotNull(objectEntityMapper.mapEntityObject(mockRuntimeInfo));
		
		
	}
	
    @Test
	public void testNullMapEntityObject(){
		
		Assert.assertNull(objectEntityMapper.mapEntityObject(null));
	}

}
