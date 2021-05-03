import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 9321))
#message = "Abstract:Cereblon (CRBN), a substrate receptor of the E3 ubiquitin ligase complex CRL4CRBN, is the target of theimmunomodulatory drugs lenalidomide and pomalidomide. Recently, it was demonstrated that binding of thesedrugs to CRBN promotes the ubiquitination and subsequent degradation of two common substrates, transcriptionfactors Aiolos and Ikaros. Here we report that the pleiotropic pathway modifier CC-122, a new chemical entitytermed pleiotropic pathway modifier binds CRBN and promotes degradation of Aiolos and Ikaros in diffuse largeB-cell lymphoma (DLBCL) and T cells in vitro, in vivo and in patients, resulting in both cell autonomous as well asimmunostimulatory effects. In DLBCL cell lines, CC-122-induced degradation or shRNA mediated knockdown ofAiolos and Ikaros correlates with increased transcription of interferon stimulated genes (ISGs) independent ofinterferon α, β, γ production and/or secretion and results in apoptosis in both ABC and GCB-DLBCL cell lines. Ourresults provide mechanistic insight into the cell of origin independent anti-lymphoma activity of CC-122, in contrastto the ABC subtype selective activity of lenalidomide."
message = "Abstract:Cereblon (CRBN), a substrate receptor of the E3 ubiquitin ligase complex CRL4CRBN, is the target of theimmunomodulatory drugs lenalidomide and pomalidomide. Recently, it was demonstrated that binding of thesedrugs to CRBN promotes the ubiquitination and subsequent degradation of two common substrates, transcriptionfactors Aiolos and Ikaros. Here we report that the pleiotropic pathway modifier CC-122, a new chemical entitytermed pleiotropic pathway modifier binds CRBN and promotes degradation of Aiolos and Ikaros in diffuse largeB-cell lymphoma (DLBCL) and T cells in vitro, in vivo and in patients, resulting in both cell autonomous as well asimmunostimulatory effects. In DLBCL cell lines, CC-122-induced degradation or shRNA mediated knockdown ofAiolos and Ikaros correlates with increased transcription of interferon stimulated genes (ISGs) independent ofinterferon production and/or secretion and results in apoptosis in both ABC and GCB-DLBCL cell lines. Ourresults provide mechanistic insight into the cell of origin independent anti-lymphoma activity of CC-122, in contrastto the ABC subtype selective activity of lenalidomide."
message = message.encode("utf-8")
length_str = f"{str(len(message))}\n".encode("utf-8")
print(length_str)
for i in range(10000):
    print(f"sending message {i}")
    s.send(length_str)
    s.send(message)
    msg = s.recv(1024)
    print(msg)
s.close()