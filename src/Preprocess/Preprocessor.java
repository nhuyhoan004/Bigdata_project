package Preprocess;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


public class Preprocessor {

    protected StanfordCoreNLP nlp;
    protected List<String> stopWords;
    protected List<String> punctuations;

    public Preprocessor() {
        Properties props;
        props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        this.nlp = new StanfordCoreNLP(props);
        this.stopWords = getStopWords();
        this.punctuations = getPunctuation();
    }

    public List<String> lemmatize(String documentText, Boolean UseStopWord)
    {
        List<String> lemmas = new LinkedList<>();

        Annotation document = new Annotation(documentText);

        this.nlp.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {

            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {

                String lemma = token.get(LemmaAnnotation.class);
                if(UseStopWord){

                    if(!this.stopWords.contains(lemma) && !isContainPunctuation(lemma)){
                        lemmas.add(lemma);
                    }

                }else{
                    if(!isContainPunctuation(lemma)){
                        lemmas.add(lemma);
                    }
                }

            }
        }

        return lemmas;
    }

    public List<String> lemmatize(String documentText){
        Boolean UseStopWord = true;
        return lemmatize(documentText, UseStopWord);
    }

    public static List<String> getStopWords(){
        InputStream inputStream = Preprocessor.class.getResourceAsStream("/special_tokens/stopwords.txt");

        List<String> stopWords = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return stopWords;
    }
    public static List<String> getPunctuation(){
        InputStream inputStream = Preprocessor.class.getResourceAsStream("/special_tokens/punctuation.txt");

        List<String> punctuations = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                punctuations.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return punctuations;
    }

    public boolean isContainPunctuation(String a){
        boolean result = false;
        for(String punctuation: this.punctuations) {
            if (a.contains(punctuation)) {
                result = true;
                break;
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String text1 = "U, dun say, so early hor... U c already then say...,,,";
        String text2 = "Nah I don't think he goes to usf, he lives around here though";

        Preprocessor Lemmatizer = new Preprocessor();
        System.out.println(Lemmatizer.lemmatize(text1));
        System.out.println(Lemmatizer.lemmatize(text2));

    }
}
