package com.ipiecoles.batch.dbExport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileFooterCallback;

import java.io.IOException;
import java.io.Writer;

public class FooterCustom implements FlatFileFooterCallback {


    private final CommuneRepository communeRepository;

    public FooterCustom(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }


    @Override
    public void writeFooter(Writer writer) throws IOException {
        writer.write("Total communes : " + communeRepository.countDistinctNom() );
    }


}


