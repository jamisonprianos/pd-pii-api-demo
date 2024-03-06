import fs from 'fs';
import fetch from 'node-fetch';
import yargs from 'yargs';
import { v4 as uuidv4 } from 'uuid';

const config = {};

const uploadWorkfile = async (inputStream, contentType, extension) => {
  const url = `${config.PrizmDocServer}/PCCIS/V1/WorkFile?FileExtension=${extension}`;
  const response = await fetch(url, {
    body: inputStream,
    headers: {
      'content-type': contentType
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Workfile creation failed');
  }
  const body = await response.json();
  return body.fileId;
};

const awaitProcessCompletion = async (processUrl, processId) => {
  const url = `${config.PrizmDocServer}/${processUrl}/${processId}`;
  const response = await fetch(url);
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error(`Checking status of ${processUrl} failed`);
  }
  const results = await response.json();
  const { output, state } = results;
  if (state === 'processing') {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    return awaitProcessCompletion(processUrl, processId);
  }
  if (state === 'complete') {
    return output;
  }
  console.dir(results, { depth: null });
  throw new Error(`${processUrl} process state unexpected: ${state}`);
};

const createSearchablePdf = async (inputWorkfileId) => {
  const url = `${config.PrizmDocServer}/v2/contentConverters`;
  const input = {
    sources: [
      { fileId: inputWorkfileId }
    ],
    dest: {
      format: 'pdf',
      pdfOptions: {
        ocr: {
          language: 'english'
        }
      }
    }
  };
  const response = await fetch(url, {
    body: JSON.stringify({ input }),
    headers: {
      'content-type': 'application/json;charset=utf-8'
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Content conversion process creation failed');
  }
  const { processId } = await response.json();
  const output = await awaitProcessCompletion('v2/contentConverters', processId);
  const { results } = output;
  return results[0].fileId;
};

const createFlattenedPdf = async (inputWorkfileId) => {
  const url = `${config.PrizmDocServer}/v2/contentConverters`;
  const input = {
    sources: [
      { fileId: inputWorkfileId }
    ],
    dest: {
      format: 'tiff'
    }
  };
  const response = await fetch(url, {
    body: JSON.stringify({ input }),
    headers: {
      'content-type': 'application/json;charset=utf-8'
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Content conversion process creation failed');
  }
  const { processId } = await response.json();
  const output = await awaitProcessCompletion('v2/contentConverters', processId);
  const { results } = output;
  return results[0].fileId;
};

const createSearchContext = async (workfileId) => {
  const url = `${config.PrizmDocServer}/v2/searchContexts`;
  const input = {
    documentIdentifier: uuidv4(),
    fileId: workfileId,
    source: 'workFile'
  };
  const response = await fetch(url, {
    body: JSON.stringify({ input }),
    headers: {
      'content-type': 'application/json;charset=utf-8'
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Search context creation failed');
  }
  const { contextId } = await response.json();
  await awaitProcessCompletion('v2/searchContexts', contextId);
  return contextId;
};

const performPiiSearch = async (contextId) => {
  const url = `${config.PrizmDocServer}/v2/piiDetectors`;
  const input = { contextId };
  const response = await fetch(url, {
    body: JSON.stringify({ input }),
    headers: {
      'content-type': 'application/json;charset=utf-8'
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('PII detector creation failed');
  }
  const { processId } = await response.json();
  await awaitProcessCompletion('v2/piiDetectors', processId);
  
  const getUrl = `${config.PrizmDocServer}/v2/piiDetectors/${processId}/entities`;
  const getResponse = await fetch(getUrl);
  if (!getResponse.ok) {
    const error = await getResponse.json();
    console.error(error);
    throw new Error('PII detection process failed');
  }
  const { entities } = await getResponse.json();
  return entities;
};

const createMarkupLayer = async (entities) => {
  // console.dir(entities, { depth: null });
  // process.exit(1);
  const markupData = {
    marks: entities.reduce((result, entity) => ([
      ...result,
      ...entity.lineGroups[0].lines.map((rect) => ({
        uid: uuidv4(),
        interactionMode: 'SelectionDisabled',
        pageNumber: entity.pageIndex + 1,
        type: 'RectangleAnnotation',
        creationDateTime: '2024-01-01T00:00:00.000Z',
        modificationDateTime: '2024-01-01T00:00:00.000Z',
        data: {},
        rectangle: rect,
        pageData: entity.lineGroups[0].pageData,
        borderColor: '#000000',
        borderThickness: 4,
        fillColor: '#000000',
        opacity: 255
      }))
    ]), [])
  };
  const body = Buffer.from(JSON.stringify(markupData, null, 2), { encoding: 'utf-8' });
  return uploadWorkfile(body, 'application/octet-stream', 'json');
};

const burnMarkup = async (documentFileId, markupFileId) => {
  const url = `${config.PrizmDocServer}/PCCIS/V1/MarkupBurner`;
  const input = { documentFileId, markupFileId };
  const response = await fetch(url, {
    body: JSON.stringify({ input }),
    headers: {
      'content-type': 'application/json;charset=utf-8'
    },
    method: 'POST'
  });
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Markup burner creation failed');
  }
  const { processId } = await response.json();
  const output = await awaitProcessCompletion('PCCIS/V1/MarkupBurner', processId);
  return output.documentFileId;
};

const getWorkfileBytes = async (workfileId) => {
  const url = `${config.PrizmDocServer}/PCCIS/V1/WorkFile/${workfileId}`;
  const response = await fetch(url);
  if (!response.ok) {
    const error = await response.json();
    console.error(error);
    throw new Error('Retrieving workfile bytes failed');
  }
  return response.blob();
};

// Validate command line arguments
yargs(process.argv.slice(2))
  .option('pd', {
    alias: 'p',
    type: 'string',
    description: 'Root URL to your PrizmDoc server (no trailing slash)'
  })
  .option('in', {
    alias: 'i',
    type: 'string',
    description: 'Relative path to your input file (must be .pdf)'
  })
  .option('out', {
    alias: 'o',
    type: 'string',
    description: 'Relative path to your output file (must be .pdf)'
  })
  .demandOption(['pd', 'in', 'out'])
  .command('$0', 'run the demo', async (args) => {
    // Update top-level configuration
    config.PrizmDocServer = args.argv.pd;

    // Upload raster input file to a PrizmDoc Workfile
    const inputStream = fs.createReadStream(args.argv.in);
    const inputWorkfileId = await uploadWorkfile(inputStream, 'application/pdf', 'pdf');
    console.log({ inputWorkfileId });

    // Convert to a searchable PDF using OCR service
    const searchableWorkfileId = await createSearchablePdf(inputWorkfileId);
    console.log({ searchableWorkfileId });

    // Get a search context for the new document
    const searchContextId = await createSearchContext(searchableWorkfileId);
    console.log({ searchContextId });

    // Search for PII data within the document
    const piiEntities = await performPiiSearch(searchContextId);
    console.dir({ piiEntityCount: piiEntities.length }, { depth: null });

    // Create a markup layer from the PII entities
    const markupFileId = await createMarkupLayer(piiEntities);
    console.log({ markupFileId });

    // Burn the generated markup data to the searchable PDF
    const burnedDocumentId = await burnMarkup(searchableWorkfileId, markupFileId);
    console.log({ burnedDocumentId });

    // Flatten (rasterize) the PDF to secure the redacted content
    const flattenedWorkfileId = await createFlattenedPdf(burnedDocumentId);
    console.log({ flattenedWorkfileId });

    // Convert back to a searchable PDF using OCR service
    const finalWorkfileId = await createSearchablePdf(flattenedWorkfileId);
    console.log({ finalWorkfileId });

    // Retrieve the document bytes for the finished document
    const burnedDocumentBytes = await getWorkfileBytes(finalWorkfileId);
    const array = await burnedDocumentBytes.arrayBuffer();
    fs.writeFileSync(args.argv.out, Buffer.from(array));
    console.log({ outputFile: args.argv.out });
  })
  .parse();
