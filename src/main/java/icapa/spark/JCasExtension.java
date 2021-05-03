package icapa.spark;

import org.apache.uima.cas.*;
import org.apache.uima.cas.admin.CASAdminException;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.LowLevelCAS;
import org.apache.uima.cas.impl.LowLevelIndexRepository;
import org.apache.uima.cas.text.AnnotationIndex;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JFSIndexRepository;
import org.apache.uima.jcas.cas.*;
import org.apache.uima.jcas.tcas.Annotation;

import java.io.InputStream;
import java.util.Iterator;
import java.util.ListIterator;

public class JCasExtension implements JCas {
    @Override
    public FSIndexRepository getFSIndexRepository() {
        return null;
    }

    @Override
    public LowLevelIndexRepository getLowLevelIndexRepository() {
        return null;
    }

    @Override
    public CAS getCas() {
        return null;
    }

    @Override
    public CASImpl getCasImpl() {
        return null;
    }

    @Override
    public LowLevelCAS getLowLevelCas() {
        return null;
    }

    @Override
    public TOP_Type getType(int i) {
        return null;
    }

    @Override
    public Type getCasType(int i) {
        return null;
    }

    @Override
    public TOP_Type getType(TOP top) {
        return null;
    }

    @Override
    public Type getRequiredType(String s) throws CASException {
        return null;
    }

    @Override
    public Feature getRequiredFeature(Type type, String s) throws CASException {
        return null;
    }

    @Override
    public Feature getRequiredFeatureDE(Type type, String s, String s1, boolean b) {
        return null;
    }

    @Override
    public void putJfsFromCaddr(int i, FeatureStructure featureStructure) {

    }

    @Override
    public <T extends TOP> T getJfsFromCaddr(int i) {
        return null;
    }

    @Override
    public void checkArrayBounds(int i, int i1) {

    }

    @Override
    public void throwFeatMissing(String s, String s1) {

    }

    @Override
    public Sofa getSofa(SofaID sofaID) {
        return null;
    }

    @Override
    public Sofa getSofa() {
        return null;
    }

    @Override
    public JCas createView(String s) throws CASException {
        return null;
    }

    @Override
    public JCas getJCas(Sofa sofa) throws CASException {
        return null;
    }

    @Override
    public JFSIndexRepository getJFSIndexRepository() {
        return null;
    }

    @Override
    public TOP getDocumentAnnotationFs() {
        return null;
    }

    @Override
    public StringArray getStringArray0L() {
        return null;
    }

    @Override
    public IntegerArray getIntegerArray0L() {
        return null;
    }

    @Override
    public FSArray getFSArray0L() {
        return null;
    }

    @Override
    public void processInit() {

    }

    @Override
    public JCas getView(String s) throws CASException {
        return null;
    }

    @Override
    public JCas getView(SofaFS sofaFS) throws CASException {
        return null;
    }

    @Override
    public TypeSystem getTypeSystem() throws CASRuntimeException {
        return null;
    }

    @Override
    public SofaFS createSofa(SofaID sofaID, String s) {
        return null;
    }

    @Override
    public FSIterator<SofaFS> getSofaIterator() {
        return null;
    }

    @Override
    public <T extends FeatureStructure> FSIterator<T> createFilteredIterator(FSIterator<T> fsIterator, FSMatchConstraint fsMatchConstraint) {
        return null;
    }

    @Override
    public ConstraintFactory getConstraintFactory() {
        return null;
    }

    @Override
    public FeaturePath createFeaturePath() {
        return null;
    }

    @Override
    public FSIndexRepository getIndexRepository() {
        return null;
    }

    @Override
    public <T extends FeatureStructure> ListIterator<T> fs2listIterator(FSIterator<T> fsIterator) {
        return null;
    }

    @Override
    public void reset() throws CASAdminException {

    }

    @Override
    public String getViewName() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public FeatureValuePath createFeatureValuePath(String s) throws CASRuntimeException {
        return null;
    }

    @Override
    public void setDocumentText(String s) throws CASRuntimeException {

    }

    @Override
    public void setSofaDataString(String s, String s1) throws CASRuntimeException {

    }

    @Override
    public String getDocumentText() {
        return null;
    }

    @Override
    public String getSofaDataString() {
        return null;
    }

    @Override
    public void setDocumentLanguage(String s) throws CASRuntimeException {

    }

    @Override
    public String getDocumentLanguage() {
        return null;
    }

    @Override
    public void setSofaDataArray(FeatureStructure featureStructure, String s) throws CASRuntimeException {

    }

    @Override
    public FeatureStructure getSofaDataArray() {
        return null;
    }

    @Override
    public void setSofaDataURI(String s, String s1) throws CASRuntimeException {

    }

    @Override
    public String getSofaDataURI() {
        return null;
    }

    @Override
    public InputStream getSofaDataStream() {
        return null;
    }

    @Override
    public String getSofaMimeType() {
        return null;
    }

    @Override
    public void addFsToIndexes(FeatureStructure featureStructure) {

    }

    @Override
    public void removeFsFromIndexes(FeatureStructure featureStructure) {

    }

    @Override
    public void removeAllIncludingSubtypes(int i) {

    }

    @Override
    public void removeAllExcludingSubtypes(int i) {

    }

    @Override
    public AnnotationIndex<Annotation> getAnnotationIndex() {
        return null;
    }

    @Override
    public <T extends Annotation> AnnotationIndex<T> getAnnotationIndex(Type type) throws CASRuntimeException {
        return null;
    }

    @Override
    public <T extends Annotation> AnnotationIndex<T> getAnnotationIndex(int i) throws CASRuntimeException {
        return null;
    }

    @Override
    public <T extends Annotation> AnnotationIndex<T> getAnnotationIndex(Class<T> aClass) throws CASRuntimeException {
        return null;
    }

    @Override
    public <T extends TOP> FSIterator<T> getAllIndexedFS(Class<T> aClass) {
        return null;
    }

    @Override
    public Iterator<JCas> getViewIterator() throws CASException {
        return null;
    }

    @Override
    public Iterator<JCas> getViewIterator(String s) throws CASException {
        return null;
    }

    @Override
    public AutoCloseable protectIndexes() {
        return null;
    }

    @Override
    public void protectIndexes(Runnable runnable) {

    }

    @Override
    public <T extends TOP> FSIndex<T> getIndex(String s, Class<T> aClass) {
        return null;
    }

    @Override
    public void release() {

    }
}
