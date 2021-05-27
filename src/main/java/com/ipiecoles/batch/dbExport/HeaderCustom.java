package com.ipiecoles.batch.dbExport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

public class HeaderCustom implements FlatFileHeaderCallback {

    private final CommuneRepository communeRepository;

    public HeaderCustom(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }


    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write("Total codes postaux : " + communeRepository.countDistinctCodePostal());
    }
}
